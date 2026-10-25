'use strict';
/* 原看板的数据适配：服务端完成统计；浏览器保存轻量实体目录、当前视图和用户已展开的汇总页。 */
let v2Status = null, v2View = null, v2Controller = null, coldRetryTimer = null;
let statusReachable = null, statusRequestSeq = 0;
let viewGeneration = 0, restoringSelection = true, manualRefresh = null, pendingQueryKey = null;
let globalTotalsCache = null;
const RETRY_WAIT_MS=180000, RETRY_DELAY_MS=5000, NOTICE_DELAY_MS=60000;
let recoveryUntil=0, recoveryBase=null, loadFailureSince=null, scanIssueSince=null;
const catalogs = {source:{items:[],next:null,total:'0'},model:{items:[],next:null,total:'0'}};
const labels = {source:new Map(),model:new Map()};
const modelColorIndices = new Map();
const previousCatalogIds = {source:null,model:null};
const catalogSequences={source:0,model:0};
const filterPanels = {source:null,model:null};
const METRIC_IDS = {total:'total',input:'input_uncached',cr:'cache_read',cw:'cache_write',output:'output_combined',reason:'reasoning_output'};
const unknownMetric = () => ({state:'unknown',known_value:null});
const words = (zh,en) => state.lang === 'en-US' ? en : zh;
const selected = (sel,id) => !sel || (sel.exclude ? !sel.has(id) : sel.has(id));
const selector = (sel,dim) => {
  if(!sel)return {mode:'all'};
  const ids=[...sel];
  // The UI keeps the original checked set; large selections use its equivalent complement on the wire.
  if(dim&&ids.length>10000)return {mode:sel.exclude?'include':'exclude',ids:catalogItems(dim).map(x=>x.id).filter(id=>!sel.has(id))};
  return {mode:sel.exclude?'exclude':'include',ids};
};
const catalogItems = dim => catalogs[dim].items;
const stampKey = stamp => JSON.stringify(stamp);
const setScanningText = txt => { $('#scanning-txt').textContent = txt; };

// Only chart geometry becomes Number. Text, tooltips, totals and ratios use decimal strings/BigInt.
function formatInteger(value, compact = true) {
  if (value == null) return '—';
  const n = BigInt(value), negative = n < 0n, abs = negative ? -n : n;
  const units = state.unit === 'zh'
    ? [[1000000000000n,'万亿',2],[100000000n,'亿',2],[10000n,'万',1]]
    : [[1000000000000n,'T',2],[1000000000n,'B',2],[1000000n,'M',2],[1000n,'K',1]];
  if (compact) for (const [scale,suffix,digits] of units) if (abs >= scale) {
    const base = 10n ** BigInt(digits), rounded = (abs * base + scale / 2n) / scale;
    return (negative?'-':'') + (rounded/base).toString() + '.' + (rounded%base).toString().padStart(digits,'0') + suffix;
  }
  return n.toLocaleString('en-US');
}
function metricText(value, compact = true) {
  if (!value || value.state === 'unknown' || value.known_value == null) return words('未知','Unknown');
  return formatInteger(value.known_value,compact);
}
function metricTitle(value) {
  return metricText(value,false);
}
function metricHTML(value, compact = true) {
  return `<span title="${escapeHtml(metricTitle(value))}">${escapeHtml(metricText(value,compact))}</span>`;
}
function ratioText(a,b) {
  if (!a || !b || a.known_value == null || b.known_value == null || BigInt(b.known_value) <= 0n) return '—';
  const denominator=BigInt(b.known_value),n=(BigInt(a.known_value)*1000n+denominator/2n)/denominator;
  return `${n/10n}.${n%10n}%`;
}
// Keep the original compact tooltip units without repeating data-quality labels.
function tooltipMetricText(value) {
  return metricText(value);
}
function sumMetricValues(values) {
  let total=0n,known=0,complete=true;
  for(const value of values){
    if(value?.known_value!=null&&value.state!=='unknown'){total+=BigInt(value.known_value);known++;}
    if(value?.state!=='exact'||value.known_value==null)complete=false;
  }
  return {state:complete?'exact':known?'partial':'unknown',known_value:known||complete?total.toString():null};
}
function showLoadFailure(error) {
  if(error?.name==='AbortError')return;
  loadFailureSince??=Date.now();
  if(DATA?.ready){
    if(!recoveryUntil){recoveryUntil=Date.now()+RETRY_WAIT_MS;recoveryBase=null;}
    retryInBackground();
    // Local statistics remain useful during a brief restart or connection hiccup.
    if(Date.now()-loadFailureSince<NOTICE_DELAY_MS){renderStatus();return;}
    loadFailed=true;$('#ledger-note').hidden=true;renderStatus();return;
  }
  loadFailed=Date.now()-loadFailureSince>=NOTICE_DELAY_MS;
  $('#status').textContent=t(loadFailed?'status.waiting':'status.connecting');
  $('#status').title=[loadFailed?t('status.connFail'):'',t('status.connFailScan')].filter(Boolean).join('\n');
  setScanningText(t('status.connFailScan'));$('#ledger-note').hidden=true;
}
function observeScanStatus(status) {
  const issue=status.refresh_error||(status.sources||[]).some(s=>s.error&&s.error!=='LOADING');
  if(issue)scanIssueSince??=Date.now();else scanIssueSince=null;
  return !!issue;
}
function retryInBackground() {
  if(document.hidden||manualRefresh||!recoveryUntil)return;
  clearTimeout(coldRetryTimer);
  const seq=loadSeq;
  // Long scans can finish in their own time; after a while, just check less often.
  const delay=Date.now()<recoveryUntil?RETRY_DELAY_MS:30000;
  coldRetryTimer=setTimeout(()=>{coldRetryTimer=null;if(!document.hidden&&seq===loadSeq)load(true);},delay);
}
function resumeDashboard() {
  recoveryUntil=Date.now()+RETRY_WAIT_MS;recoveryBase=v2Status?.refresh_generation??null;
  loadFailureSince=null;scanIssueSince=null;loadFailed=false;
  if(DATA?.ready){$('#ledger-note').hidden=true;renderStatus();}
  return load(true);
}
async function apiV2(path,body,signal) {
  const controller=new AbortController();
  const abort=()=>controller.abort();
  if(signal?.aborted)controller.abort();else signal?.addEventListener('abort',abort,{once:true});
  let timeout;
  try {
    return await Promise.race([
      (async()=>{
        const response=await fetch('/api/v2/'+path,{method:body?'POST':'GET',headers:body?{'Content-Type':'application/json'}:{},body:body?JSON.stringify(body):undefined,cache:'no-store',signal:controller.signal});
        let value;try{value=await response.json();}catch(e){if(e.name==='AbortError')throw e;throw new Error(`HTTP ${response.status}`);}
        if(!response.ok){const detail=typeof value.error==='object'?value.error:{code:value.error,message:value.message};const e=new Error(detail?.message||`HTTP ${response.status}`);e.code=detail?.code;throw e;}
        return value;
      })(),
      new Promise((_,reject)=>{timeout=setTimeout(()=>{controller.abort();reject(new Error(words('请求超时，请重试','Request timed out; please retry')));},15000);}),
    ]);
  } finally {clearTimeout(timeout);signal?.removeEventListener('abort',abort);}
}
// The inputs hold a draft; only a complete real calendar range reaches queries.
let customCommittedRange = null, customDateDraft = null, customDateEditing = false;
function validCalendarDate(day) {
  if(typeof day!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(day))return false;
  const date=new Date(day+'T12:00:00Z');
  return Number.isFinite(date.getTime())&&date.toISOString().slice(0,10)===day;
}
function validCustomRange(start,end) {
  return validCalendarDate(start)&&validCalendarDate(end)&&validCalendarDate(addDay(start>end?start:end,1));
}
function ensureCustomRange() {
  if(!reportTimezone()){const error=new Error('Report timezone is not ready');error.code='INVALID_CUSTOM_RANGE';throw error;}
  if(validCustomRange(state.rs,state.re))customCommittedRange=[state.rs,state.re].sort();
  if(!customCommittedRange){
    const extent=v2View?.response.payload.data.date_extent,end=validCalendarDate(extent?.last)?extent.last:localDayStr();
    const first=validCalendarDate(extent?.first)?extent.first:null,start=addDay(end,-6);
    customCommittedRange=[first&&first>start&&first<=end?first:start,end];
  }
  [state.rs,state.re]=customCommittedRange;
  if(!customDateEditing){customDateDraft={rs:state.rs,re:state.re};syncCustomDateValidity();}
  return [...customCommittedRange];
}
function syncCustomDateValidity() {
  const valid=validCustomRange(customDateDraft.rs,customDateDraft.re);
  for(const key of ['rs','re']){
    const input=$('#'+key+'-pick'),bad=!validCalendarDate(customDateDraft[key])||(!valid&&validCalendarDate(customDateDraft.rs)&&validCalendarDate(customDateDraft.re));
    input.setAttribute('aria-invalid',String(bad));input.title=bad?words('请输入有效日期','Enter a valid date'):'';
    input.setCustomValidity?.(bad?input.title:'');
  }
  return valid;
}
function editCustomDate(key,value,commit) {
  ensureCustomRange();customDateEditing=true;customDateDraft[key]=value;
  const valid=syncCustomDateValidity();
  if(!commit||!valid)return;
  const [start,end]=[customDateDraft.rs,customDateDraft.re].sort();
  const changed=state.rs!==start||state.re!==end;
  changeQueryIntent(()=>{state.rs=start;state.re=end;});
  customCommittedRange=[start,end];customDateDraft={rs:start,re:end};customDateEditing=false;
  $('#rs-pick').value=start;$('#re-pick').value=end;
  if(changed)render();
}
// Only explicit query controls reset legend intent. Background catalog recovery,
// as-of changes, midnight, formatting and chart reconstruction do not.
let doughnutIntentRevision=0;
const doughnutHiddenIds=new Set();
function queryIntentKey() {
  return JSON.stringify([state.win,state.win==='custom'?[state.rs,state.re]:null,_selKey(state.srcSel),_selKey(state.mdlSel),state.metric,state.group]);
}
function resetDoughnutIntent(){doughnutIntentRevision++;doughnutHiddenIds.clear();}
function changeQueryIntent(change){const before=queryIntentKey();change();if(before!==queryIntentKey())resetDoughnutIntent();}
function rememberDoughnutVisibility(chart) {
  if(!chart?.$entityIds||chart.$intentRevision!==doughnutIntentRevision)return;
  chart.$entityIds.forEach((id,index)=>{if(chart.getDataVisibility(index))doughnutHiddenIds.delete(id);else doughnutHiddenIds.add(id);});
}
function currentQueryKey() {
  return JSON.stringify([state.win,state.rs,state.re,localDayStr(),_selKey(state.srcSel),_selKey(state.mdlSel),state.metric,state.group,v2Status?.stamp?.report_timezone]);
}
function buildQuery(windowOverride) {
  const bounds=rangeBounds();
  return {window:windowOverride||(bounds?{kind:'calendar',start_date:bounds[0],end_date_exclusive:addDay(bounds[1],1)}:{kind:'all'}),report_timezone:v2Status.stamp.report_timezone,as_of_ms:null,sources:selector(state.srcSel,'source'),models:selector(state.mdlSel,'model'),metric:METRIC_IDS[state.metric],group_by:state.group};
}
function comparisonSpec() {
  if(state.win==='all')return null;
  if(isDayWin(state.win)&&winDay()===localDayStr())return {kind:'yesterday_same_local_time'};
  const [start,end]=rangeBounds();let from;
  if(state.win==='thisweek'||state.win==='lastweek')from=addDay(start,-7);
  else if(state.win==='thismonth'||state.win==='lastmonth'){const d=new Date(start+'T12:00:00Z');d.setUTCMonth(d.getUTCMonth()-1);from=d.toISOString().slice(0,10);}
  else from=addDay(start,-(Math.round((Date.parse(end)-Date.parse(start))/86400000)+1));
  return {kind:'explicit',window:{kind:'calendar',start_date:from,end_date_exclusive:start}};
}
function comparisonLabel() {
  if(isDayWin(state.win))return winDay()===localDayStr()?t('cmp.yesterdaySame'):t('cmp.prevDay');
  if(state.win==='thisweek')return t('cmp.lastWk');if(state.win==='lastweek')return t('cmp.prevWk');
  if(state.win==='thismonth')return t('cmp.lastMo');if(state.win==='lastmonth')return t('cmp.prevMo');
  return state.win==='custom'?t('cmp.prevSeg'):t('cmp.prevN',RANGE_DAYS_N[state.win]);
}
function registerLabels(dim,items) {
  for(const item of items){labels[dim].set(item.id,item.label);if(dim==='source')SRC_LABELS[item.id]=item.label;
    else if(item.color_index!=null)modelColorIndices.set(item.id,Number(item.color_index));}
}
async function readFullCatalog(dim,signal) {
  const sequence=++catalogSequences[dim];
  // Selection recovery must only see complete directories, even after a failed page.
  for(let attempt=0;attempt<2;attempt++){
    try{
      const items=[];let first=null,cursor=null;
      do{
        const page=await apiV2('catalog',{dimension:dim,search:'',page:{limit:200,cursor}},signal);
        if(signal?.aborted||sequence!==catalogSequences[dim])return false;
        if(first)verifyStamp(first,page);else first=page;
        items.push(...page.items);cursor=page.next_cursor;
      }while(cursor);
      registerLabels(dim,items);
      catalogs[dim]={items,next:null,total:first.total_count,stamp:first.stamp};
      return true;
    }catch(e){
      if(signal?.aborted||sequence!==catalogSequences[dim])return false;
      if(attempt===0&&['CURSOR_STALE','TIMEZONE_CHANGED'].includes(e.code))continue;
      throw e;
    }
  }
}
function syncSelections() {
  for(const dim of ['source','model']){
    const key=dim==='source'?'srcSel':'mdlSel',ids=catalogItems(dim).map(x=>x.id),previous=previousCatalogIds[dim];
    let sel=state[key],solo=state[dim==='source'?'srcSolo':'mdlSolo'];
    // Stable model IDs contain case-sensitive base64url; only source IDs have legacy case aliases.
    const matchKey=id=>dim==='source'?id.toLowerCase():id;
    if(solo!==null){
      const match=ids.find(id=>matchKey(id)===matchKey(solo));
      // Source catalogs depend on the cutoff and successful source scans, so even
      // a complete first directory cannot prove that a saved target is invalid.
      // Models are invalidated only by the global server check after the query.
      solo=match??solo;setSelection(dim,new Set([solo]),solo);
      previousCatalogIds[dim]=new Set(ids);
      continue;
    }
    if(sel&&ids.length){
      if(sel.exclude)sel=new Set(ids.filter(id=>!sel.has(id)));
      if(sel.size){
        const known=new Map(ids.map(id=>[matchKey(id),id]));
        const alive=new Set([...sel].map(id=>known.get(matchKey(id))).filter(Boolean));
        if(previous)for(const id of ids)if(!previous.has(id))alive.add(id);
        sel=alive.size===0?(previous||dim==='model'?sel:null):alive.size===ids.length?null:alive;
      }
      setSelection(dim,sel);
    }
    previousCatalogIds[dim]=new Set(ids);
  }
}
function migrateModels() {
  if(!state.mdlSel)return;
  const next=new Set();next.exclude=!!state.mdlSel.exclude;
  const migrate=name=>{
    if(name==='m1:u'||name.startsWith('m1:n:'))return name;
    const normalized=name.toLowerCase(),bytes=new TextEncoder().encode(normalized);
    const encoded=btoa(Array.from(bytes,b=>String.fromCharCode(b)).join('')).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
    const id=name?'m1:n:'+encoded:'m1:u';labels.model.set(id,name);return id;
  };
  for(const name of state.mdlSel)next.add(migrate(name));
  setSelection('model',next,state.mdlSolo===null?null:migrate(state.mdlSolo));
}
async function fetchDashboard(signal) {
  const query=buildQuery(),key=currentQueryKey(), trend=state.win.includes('week')?{unit:'week',periods:10,anchor_date:localDayStr()}:state.win.includes('month')?{unit:'month',periods:12,anchor_date:localDayStr()}:null;
  const response=await apiV2('query',{kind:'dashboard',query,series_unit:'day',max_points:10000,table_limit:100,comparison:comparisonSpec(),trend},signal);
  const payload=response.payload.data, fixed={...query,as_of_ms:response.as_of_ms}, extra=[];
  let day=payload.latest_day,sourceTable=payload.source_table,modelTable=payload.model_table;
  // Future-dated facts and daily evidence can become eligible as time advances
  // without changing the data stamp. Totals must share this query's cutoff.
  const globalKey=stampKey(response.stamp)+'|'+response.as_of_ms;
  const isGlobal=query.window.kind==='all'&&query.sources.mode==='all'&&query.models.mode==='all';
  let globalMetrics=isGlobal?payload.totals.metrics:globalTotalsCache?.key===globalKey?globalTotalsCache.metrics:null;
  if(isGlobal&&!signal?.aborted)globalTotalsCache={key:globalKey,metrics:globalMetrics};
  if(!globalMetrics)extra.push(apiV2('query',{kind:'totals',query:{...fixed,window:{kind:'all'},sources:{mode:'all'},models:{mode:'all'},group_by:'none',metric:'total'}},signal).then(r=>{
    verifyStamp(response,r);globalMetrics=r.payload.data.metrics;
    if(!signal?.aborted)globalTotalsCache={key:globalKey,metrics:globalMetrics};
  }));
  if(isDayWin(state.win)&&day?.date!==winDay()){
    const date=winDay();extra.push(apiV2('query',{kind:'day',query:{...fixed,window:{kind:'calendar',start_date:date,end_date_exclusive:addDay(date,1)}},date},signal).then(r=>{verifyStamp(response,r);day=r.payload.data;}));
  }
  // Detail rows always rank by total tokens, independent of the selected chart metric.
  if(query.metric!=='total')for(const dimension of ['source','model'])extra.push(apiV2('query',{kind:'table',query:{...fixed,metric:'total'},dimension,scope:'facet',page:{limit:100,cursor:null}},signal).then(r=>{verifyStamp(response,r);if(dimension==='source')sourceTable=r.payload.data;else modelTable=r.payload.data;}));
  await Promise.all(extra);
  // A background refresh keeps rows the user already reached by scrolling.
  if(v2View?.key===key)for(const dim of ['source','model']){
    const page=dim==='source'?sourceTable:modelTable,target=v2View.tables[dim].rows.length;
    while(page.next_cursor&&page.rows.length<target){
      const r=await apiV2('query',{kind:'table',query:{...fixed,metric:'total'},dimension:dim,scope:'facet',page:{limit:100,cursor:page.next_cursor}},signal);
      verifyStamp(response,r);page.rows.push(...r.payload.data.rows);page.next_cursor=r.payload.data.next_cursor;
    }
  }
  return {key,query:fixed,response,day,globalMetrics,tables:{source:sourceTable,model:modelTable},tableBusy:new Set()};
}
function verifyStamp(expected,actual) {
  if(stampKey(expected.stamp)!==stampKey(actual.stamp)){const e=new Error(words('数据已更新，正在重新读取','Data changed; reloading'));e.code='CURSOR_STALE';throw e;}
}
async function load(passive) {
  const seq=++loadSeq;pendingQueryKey=currentQueryKey();v2Controller?.abort();const controller=v2Controller=new AbortController();
  clearTimeout(coldRetryTimer);coldRetryTimer=null;
  try {
    let status=await readStatusV2('status?arf='+(passive===true?'':state.arf),controller.signal,()=>seq===loadSeq);
    if(seq!==loadSeq)return {ok:false,superseded:true};
    let scanIssue=observeScanStatus(status);
    registerLabels('source',(status.sources||[]).map(s=>({id:s.id,label:s.label||s.id})));
    if(!status.ready){
      loadFailureSince=null;loadFailed=false;
      if(!DATA){
        $('#app').hidden=true;$('#scanning').hidden=false;setScanningText(status.refresh_error?t('status.scanFailRetry'):t('scan.first'));
        coldRetryTimer=setTimeout(()=>{if(!document.hidden)load(true);},status.refresh_error?5000:3000);
      }else{
        renderStatus();if(!recoveryUntil){recoveryUntil=Date.now()+RETRY_WAIT_MS;recoveryBase=status.refresh_generation;}
        retryInBackground();
      }
      return {ok:false,pending:true};
    }
    // Source membership is as-of dependent; model names are snapshot metadata.
    const changedCatalogs=['source','model'].filter(dim=>dim==='source'||!catalogs[dim].stamp||stampKey(catalogs[dim].stamp)!==stampKey(status.stamp));
    if(changedCatalogs.length){
      const completed=await Promise.all(changedCatalogs.map(dim=>readFullCatalog(dim,controller.signal)));
      if(seq!==loadSeq||controller.signal.aborted||completed.some(done=>!done))return {ok:false,superseded:true};
      syncSelections();
    }
    pendingQueryKey=currentQueryKey();
    let next;
    for(let attempt=0;attempt<2;attempt++){
      try{next=await fetchDashboard(controller.signal);break;}
      catch(e){if(attempt===0&&['CURSOR_STALE','TIMEZONE_CHANGED'].includes(e.code)){status=await readStatusV2('status?arf=',controller.signal,()=>seq===loadSeq);scanIssue=observeScanStatus(status);continue;}throw e;}
    }
    if(seq!==loadSeq||controller.signal.aborted||next.key!==currentQueryKey())return {ok:false,superseded:true};
    if(restoringSelection){
      // UNKNOWN_SOURCE uses the static source registry; directory absence alone is insufficient.
      const clearSource=state.srcSolo!==null&&next.response.warnings.includes('UNKNOWN_SOURCE:'+state.srcSolo);
      let clearModel=false;
      if(state.mdlSel&&!state.mdlSel.exclude&&state.mdlSel.size&&next.response.warnings.includes('ALL_SELECTED_MODELS_UNKNOWN')){
        // A failed source can hide its models, even when that source is not selected.
        // Check all sources at this snapshot/cutoff without reusing cached coverage.
        const confirmation=await apiV2('query',{kind:'totals',query:{...next.query,window:{kind:'all'},sources:{mode:'all'},metric:'total',group_by:'none'}},controller.signal);
        if(seq!==loadSeq||controller.signal.aborted||next.key!==currentQueryKey())return {ok:false,superseded:true};
        verifyStamp(next.response,confirmation);
        clearModel=confirmation.source_coverage==='complete'&&confirmation.warnings.includes('ALL_SELECTED_MODELS_UNKNOWN');
      }
      if(clearSource||clearModel){
        restoringSelection=false;
        if(clearSource)setSelection('source',null);
        if(clearModel)setSelection('model',null);
        return load(true);
      }
    }
    restoringSelection=false;v2View=next;loadFailed=false;loadFailureSince=null;
    registerLabels('source',next.tables.source.rows);registerLabels('model',next.tables.model.rows);
    if(state.metric==='cw'&&!cwVisible()){state.metric='total';segSync('#seg-metric',v=>v==='total');return load(true);}
    DATA={ready:true,generated_at_ms:Number(status.generated_at_ms||next.response.as_of_ms),view_generation:++viewGeneration,last_scan_failed:!!status.refresh_error,version:status.version,sources:status.sources||[]};
    if(!status.refresh_error&&BigInt(status.refresh_generation||'0')!==BigInt(rescanTimedOutGen||'0'))rescanTimedOut=false;
    $('#scanning').hidden=true;$('#app').hidden=false;
    prepareDays();next.key=currentQueryKey();renderPayload();
    if(!recoveryUntil&&(status.refreshing||scanIssue)){recoveryUntil=Date.now()+RETRY_WAIT_MS;recoveryBase=status.refresh_generation;}
    if(recoveryUntil){
      if(!status.refreshing&&!scanIssue&&(recoveryBase===null||status.refresh_generation!==recoveryBase)){recoveryUntil=0;recoveryBase=null;clearTimeout(coldRetryTimer);coldRetryTimer=null;}
      else retryInBackground();
    }
    return {ok:true,status};
  } catch(e){
    if(seq!==loadSeq||e.name==='AbortError')return {ok:false,superseded:true};
    if(e.code==='INVALID_CUSTOM_RANGE')return {ok:false,invalidRange:true};
    showLoadFailure(e);if(!DATA)coldRetryTimer=setTimeout(()=>{if(!document.hidden)load(true);},5000);
    return {ok:false};
  }
  finally {if(seq===loadSeq)pendingQueryKey=null;}
}
function render() {
  setWinSegUi();renderSelectorButtons();
  if(v2View&&v2View.key===currentQueryKey()){renderPayload();return;}
  if(pendingQueryKey!==currentQueryKey())load(true);
}
function prepareDays() {
  const extent=v2View.response.payload.data.date_extent;
  daysAll=[extent.first,extent.last].filter(Boolean);
  const today=localDayStr(), first=extent.first||'', last=extent.last||'';
  for(const id of ['day-pick','rs-pick','re-pick']){const el=$('#'+id);el.min=first;el.max=id==='day-pick'&&last&&last<today?today:last;}
  ensureCustomRange();
  for(const[id,value]of [['day-pick',isDayWin(state.win)?winDay():''],['rs-pick',customDateDraft.rs],['re-pick',customDateDraft.re]])if(document.activeElement!==$('#'+id))$('#'+id).value=value||'';
  if(isDayWin(state.win)){
    $('#day-prev').disabled=!first||addDay(winDay(),-1)<first;
    $('#day-next').disabled=!last||addDay(winDay(),1)>last;
  }
}
function cwVisible() {
  const m=v2View?.globalMetrics;if(!m)return true;
  if(m.cache_write.known_value==null||m.total.known_value==null)return true;
  return BigInt(m.cache_write.known_value)*1000n>=BigInt(m.total.known_value);
}
function renderPayload() {
  renderedDay=localDayStr();prepareDays();setWinSegUi();renderSelectorButtons();
  $('#seg-metric [data-v="cw"]').hidden=!cwVisible()&&state.metric!=='cw';
  renderCards();renderStatus();renderCharts();renderModelTable();renderSourceTable();
  if(!loadFailed){$('#ledger-note').textContent='';$('#ledger-note').hidden=true;}
  for(const dimension of ['source','model']){
    const panel=filterPanels[dimension];if(panel&&!panel.element.hidden){
      if(panel.key!==filterWindowKey())buildFilterPanel(dimension);else renderFilterList(dimension);
    }
  }
  requestAnimationFrame(()=>{for(const chart of Object.values(charts))try{chart.resize();}catch{}loadVisibleTablePages();});
}
function renderCards() {
  const d=v2View.response.payload.data,m=d.totals.metrics, none=(state.srcSel&&!state.srcSel.exclude&&!state.srcSel.size)||(state.mdlSel&&!state.mdlSel.exclude&&!state.mdlSel.size);
  const sub=(none?t('cards.noneSel'):state.win==='today'?t('seg.today'):rangeLabel())+' · '+t('cards.requests',metricText(m.requests,false))+((state.srcSel||state.mdlSel)&&!none?t('cards.filtered'):'');
  // 对比段是已转义 HTML:文本部分过 escapeHtml,仅注入受控 .delta 方向片;三角 aria-hidden,方向语义由 +/- 承载。
  // 方向按取整后的百分比取号(取整为 0 视为持平)。两侧 exact 才显示百分比;任一侧 partial(live 来源缺字段的下界值,
  // 实机常态)只给方向、title 注明按已知部分比较;任一侧 unknown 不给方向
  let comparison='';const previous=d.comparison;
  if(previous?.comparable){
    const a=m.total,b=previous.totals.metrics.total,label=comparisonLabel();
    const known=a.known_value!=null&&b.known_value!=null&&a.state!=='unknown'&&b.state!=='unknown';
    const exact=known&&a.state==='exact'&&b.state==='exact';
    let dir=0,rounded=0n,before=0n;
    if(known){const current=BigInt(a.known_value);before=BigInt(b.known_value);
      if(before>0n){const diff=current-before;rounded=diff<0n?-((-diff*100n+before/2n)/before):(diff*100n+before/2n)/before;dir=rounded>0n?1:rounded<0n?-1:0;}
      else if(current>0n)dir=1;}
    const arrow=(inner,title)=>`<span class="delta ${dir>0?'up':'down'}"${title?` title="${escapeHtml(title)}"`:''}><span aria-hidden="true">${dir>0?'▲':'▼'}</span>${inner}</span>`;
    if(!exact)comparison=escapeHtml(' · '+label+': '+metricText(b))+(dir?' '+arrow('',words('按已知部分比较,方向仅供参考','Direction from known portions only; indicative')):'');
    else if(before>0n)comparison=escapeHtml(' · vs '+label+' ')+(dir?arrow(' '+(dir>0?'+':'')+rounded+'%'):'0%');
    else if(dir)comparison=escapeHtml(' · vs '+label+' ')+arrow('')+' '+escapeHtml(t('cmp.zeroPrev'));
  }
  const hasTotal=m.total.known_value!=null&&BigInt(m.total.known_value)>0n;
  const items=[[t('card.total'),m.total,sub,true,comparison],[t('card.input'),m.input_uncached,hasTotal?ratioText(m.input_uncached,m.total)+t('card.inputSub'):'—'],[t('card.cr'),m.cache_read,hasTotal?ratioText(m.cache_read,m.total)+t('card.crSub'):'—']];
  if(cwVisible())items.push([t('card.cw'),m.cache_write,hasTotal?ratioText(m.cache_write,m.total)+t('card.cwSub'):'—']);
  items.push([t('card.output'),m.output_combined,hasTotal?ratioText(m.output_combined,m.total)+t('card.reasonShare',ratioText(m.reasoning_output,m.output_combined)):'—']);
  $('#cards').innerHTML=items.map(([label,value,detail,highlight,delta])=>`<div class="card${highlight?' hl':''}"><div class="k">${escapeHtml(label)}</div><div class="v" title="${escapeHtml(metricTitle(value))}">${escapeHtml(metricText(value))}</div><div class="sub">${escapeHtml(detail)}${delta||''}</div></div>`).join('');
}
async function readStatusV2(path,signal,isCurrent) {
  const sequence=++statusRequestSeq;
  const current=()=>sequence===statusRequestSeq&&!signal?.aborted&&isCurrent();
  try {
    const result=await apiV2(path,undefined,signal);
    if(!result||typeof result!=='object'||!Array.isArray(result.sources))throw new Error('Invalid status response');
    if(current()){v2Status=result;statusReachable=true;}
    return result;
  } catch(error) {
    if(current()){statusReachable=false;}
    throw error;
  }
}
function renderStatus() {
  if(!DATA?.ready)return;
  const asOf=t('status.dataAsOf',localDate(DATA.generated_at_ms)),stale=localDayStr(new Date(DATA.generated_at_ms))!==localDayStr();
  const details=[asOf];
  const scanNotice=scanIssueSince!==null&&Date.now()-scanIssueSince>=NOTICE_DELAY_MS;
  if(loadFailed)details.push(t('st.connFailShow'));else if(v2Status?.refresh_error&&scanNotice)details.push(t('st.scanFailShow'));
  if(stale)details.push(t('st.waitData'));
  const pending=(pendingQueryKey&&pendingQueryKey!==v2View?.key)||(v2View&&v2View.key!==currentQueryKey());
  if(pending)details.push(words('正在读取所选范围…','Loading selected range…'));
  const failed=(v2Status?.sources||[]).filter(s=>s.error&&s.error!=='LOADING').length;
  if(failed&&scanNotice)details.push(t('st.srcFailed',failed));
  if(rescanTimedOut)details.push(t('st.rescanTimeout'));
  if(v2Status?.refreshing)details.push(t('st.bgScan'));
  if(rescanBusy)details.push(t('st.manualScan'));
  const isolated=(v2Status?.sources||[]).reduce((n,s)=>n+BigInt(s.isolated_objects||'0'),0n);
  if(isolated)details.push(words(`${isolated} 个文件已隔离`,`${isolated} files isolated`));
  const remaining=Math.max(0,Math.ceil((arfNextAt-Date.now())/1000));
  const refreshDetail=state.arf?t('st.arf',state.arf,remaining):t('st.arfOff');
  details.push(refreshDetail);
  // Keep the ticking digits outside the live status message and reserve their space.
  const countdown=$('#refresh-countdown');
  countdown.hidden=!state.arf;
  countdown.textContent=state.arf?t('st.nextRefresh',remaining):'';
  countdown.title=refreshDetail;
  countdown.setAttribute('aria-label',refreshDetail);
  const updating=!!(pending||v2Status?.refreshing||rescanBusy);
  const saved=loadFailed||rescanTimedOut||stale||isolated>0n||(scanNotice&&(v2Status?.refresh_error||failed));
  const el=$('#status'),text=t(updating?'st.shortUpdating':saved?'st.shortSaved':'st.shortReady');
  if(el.dataset.last!==text){el.textContent=text;el.dataset.last=text;}
  el.classList.toggle('st-scan',updating);
  el.title=[...new Set([...details,...dataNotes(),...(v2Status?.sources||[]).filter(s=>s.error).map(s=>`${srcLabel(s.id)}: ${s.error}`),v2Status?.refresh_error||''].filter(Boolean))].join('\n');
  const header=document.querySelector('header h1');if(header)header.title=t('status.dataAsOf',localDate(DATA.generated_at_ms));
  if(DATA.version)$('#ver').textContent=t('ver.note',DATA.version);
}
function dataNotes() {
  if(!v2View)return [];
  const r=v2View.response,d=r.payload.data,notes=[];
  if(r.source_coverage==='partial')notes.push(words('部分来源暂未读完，当前统计可能不完整。','Some sources are incomplete; totals may be partial.'));
  if(r.warnings.includes('LIVE_HISTORY_UNPROTECTED'))notes.push(words('部分来源仍依赖原始历史文件。','Some sources still depend on their original history files.'));
  if(r.warnings.includes('DAY_EVIDENCE_EXCLUDED'))notes.push(words('有时区或覆盖范围待确认的日汇总，暂未计入。','Daily records with unverified timezone or coverage are excluded.'));
  if(d.totals.coverage.grain==='day'||d.totals.coverage.grain==='mixed')notes.push(words('日汇总计入总量；小时图仅展示真实的小时记录。','Totals include daily records; hourly charts show only known hourly usage.'));
  if(d.series.compressed)notes.push(words('时间跨度较长，主图已按周或月合并显示。','The long date range is grouped by week or month.'));
  return notes;
}
function displayKey(key){return key.id==='__other__'?t('bucket.other'):key.label;}
function chartColor(key,index){return state.group==='source'?srcColor(key.id):key.id==='__other__'?'#8b949e':mdlStableColor(key.id);}
function drawSeries(id,series,onClick,presentation={}) {
  const keys=series?.keys||[],points=series?.points||[];
  // Keys represent actual records; a zero metric must not hide their original legends.
  const used=keys.map((key,index)=>({key,index}));
  const datasets=used.map(({key,index},i)=>{
    const color=chartColor(key,i);
    return {label:displayKey(key),data:points.map(p=>p.values[index]?.known_value==null?null:Number(p.values[index].known_value)),exactValues:points.map(p=>p.values[index]||unknownMetric()),
      backgroundColor:presentation.selectedIndex==null?color+'cc':points.map((_,pi)=>color+(pi===presentation.selectedIndex?'ee':'66')),
      borderColor:color,borderWidth:1,borderRadius:2};
  });
  const options=stackOpts(t(METRICS[state.metric].labelKey));
  options.plugins.tooltip.filter=it=>{const v=it.dataset.exactValues[it.dataIndex];return v.state!=='exact'||BigInt(v.known_value||'0')!==0n;};
  options.plugins.tooltip.callbacks.label=it=>it.dataset.label+': '+tooltipMetricText(it.dataset.exactValues[it.dataIndex]);
  options.plugins.tooltip.callbacks.title=items=>{
    if(!items.length)return '';
    const total=sumMetricValues(items.map(it=>it.dataset.exactValues[it.dataIndex]));
    return items[0].label+' · '+t('sub.total',tooltipMetricText(total));
  };
  if(onClick)options.onClick=(event,elements)=>{if(elements?.length)onClick(points[elements[0].index],elements[0].index);};
  mkChart(id,{type:'bar',data:{labels:points.map(p=>presentation.label?presentation.label(p):p.label),datasets},options});
}
function daySeries(day) {
  return {keys:day?.keys||[],points:(day?.hours||Array.from({length:24},(_,hour)=>({hour,values:[]}))).map(h=>({label:String(h.hour).padStart(2,'0')+(h.state==='nonexistent'?' ∅':h.intervals?.length>1?' ↺':''),values:h.values}))};
}
function renderCharts() {
  const d=v2View.response.payload.data,single=isDayWin(state.win);
  const reportTimezone=v2View.response.stamp.report_timezone;
  $('#day-prev').hidden=!single;$('#day-next').hidden=!single;$('#day-back').hidden=!single||state.win==='today';
  $('#hourly-panel').hidden=single;$('#grid2').classList.toggle('single',single);
  $('#daily-h2').textContent=t(single?'h2.dayHourly':'h2.daily');
  const metric=d.totals.metrics[METRIC_IDS[state.metric]];
  $('#daily-sub').textContent=single&&metric.state==='exact'&&metric.known_value==='0'?t('empty.noSel'):t(single?'sub.dayTotal':'sub.total',metricText(metric));
  drawSeries('#chartDaily',single?daySeries(v2View.day):d.series,single?null:point=>{
    if(d.series.unit==='day')enterDay(point.label);else {changeQueryIntent(()=>{state.win='custom';state.rs=dateInReportZone(point.start_ms,reportTimezone);state.re=addDay(dateInReportZone(point.end_ms_exclusive,reportTimezone),-1);});customDateEditing=false;render();}
  });
  if(!single){
    if(d.trend){
      const unit=state.win.includes('week')?'week':'month';$('#hourly-h2').textContent=t(unit==='week'?'h2.trendWk':'h2.trendMo');$('#hourly-sub').textContent=t(unit==='week'?'sub.trendWk':'sub.trendMo',unit==='week'?10:12);
      const selectedWindow=state.win,selectedIndex=d.trend.points.length-1-(['thisweek','thismonth'].includes(selectedWindow)?0:1);
      drawSeries('#chartHourly',d.trend,(point,index)=>{
        changeQueryIntent(()=>{if(index===selectedIndex)state.win=selectedWindow;
        else {state.win='custom';state.rs=dateInReportZone(point.start_ms,reportTimezone);state.re=addDay(dateInReportZone(point.end_ms_exclusive,reportTimezone),-1);}});
        customDateEditing=false;render();
      },{selectedIndex,label:point=>unit==='week'?point.label.slice(5):point.label.slice(0,7)});
    }else{
      $('#hourly-h2').textContent=t('h2.hourly');$('#hourly-sub').textContent=v2View.day?t('sub.hourlyDay',v2View.day.date,metricText(v2View.day.total.metrics[METRIC_IDS[state.metric]])):t('empty.none');
      drawSeries('#chartHourly',daySeries(v2View.day));
    }
  }
  renderDoughnut();
}
function dateInReportZone(ms,reportTimezone) {
  const parts=new Intl.DateTimeFormat('en-US',{timeZone:reportTimezone,year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(new Date(Number(ms)));
  const get=type=>parts.find(p=>p.type===type).value;return `${get('year')}-${get('month')}-${get('day')}`;
}
function renderDoughnut() {
  const d=v2View.response.payload.data.distribution,rows=d.rows,total=BigInt(d.known_total||'0'),hasSlices=rows.length>0&&total>0n;
  $('#doughnut-h2').textContent=t(state.group==='model'?'h2.modelShare':'h2.sourceShare');
  rememberDoughnutVisibility(charts['#chartSource']);
  const canvas=$('#chartSource'),empty=$('#doughnut-empty');empty.hidden=hasSlices;empty.textContent=d.complete?t('empty.noSel'):metricText(unknownMetric());
  if(canvas)canvas.style.display=hasSlices?'':'none';
  if(!hasSlices){if(charts['#chartSource']){charts['#chartSource'].destroy();delete charts['#chartSource'];}return;}
  const ids=rows.map(r=>r.id),members=new Set(ids);
  for(const id of doughnutHiddenIds)if(!members.has(id))doughnutHiddenIds.delete(id);
  const datasets=[{data:rows.map(r=>r.value.known_value==null?null:Number(r.value.known_value)),exactValues:rows.map(r=>r.value),backgroundColor:rows.map((r,i)=>chartColor(r,i)+'dd'),borderColor:'#0b0e14',borderWidth:2}];
  const options={responsive:true,maintainAspectRatio:false,cutout:'62%',plugins:{legend:{position:'bottom',labels:{boxWidth:10,boxHeight:10},onClick:(event,item,legend)=>{Chart.overrides.doughnut.plugins.legend.onClick(event,item,legend);rememberDoughnutVisibility(legend.chart);}},tooltip:{callbacks:{label:it=>tooltipMetricText(it.dataset.exactValues[it.dataIndex])}}}};
  mkChart('#chartSource',{type:'doughnut',$entityIds:ids,$intentRevision:doughnutIntentRevision,$hiddenEntityIds:new Set(doughnutHiddenIds),data:{labels:rows.map(r=>`${displayKey(r)} ${ratioText(r.value,{state:d.complete?'exact':'partial',known_value:total.toString()})}`),datasets},options,plugins:[centerTextPlugin],$center:{v:formatInteger(total),l:t('dough.center',t(METRICS[state.metric].labelKey))}});
}
function renderModelTable(){renderDetailTable('model');}
function renderSourceTable(){renderDetailTable('source');}
function sourceCacheReadShareHTML(metrics) {
  const read=metrics.cache_read,total=metrics.total;
  if(!read||!total||read.state==='unknown'||total.state==='unknown'||read.known_value==null||total.known_value==null)return '';
  const cached=BigInt(read.known_value),sum=BigInt(total.known_value);
  if(cached<=0n||sum<=0n||cached>sum)return '';
  return ` <span style="color:var(--muted)">(${ratioText(read,total)})</span>`;
}
function renderDetailTable(dim) {
  const page=v2View.tables[dim],model=dim==='model',cw=cwVisible(),sel=model?state.mdlSel:state.srcSel;
  let html=model?`<tr><th>${t('tbl.model')}</th><th style="text-align:left">${t('tbl.source')}</th><th>${t('tbl.input')}</th><th>${t('tbl.cr')}</th>${cw?`<th>${t('tbl.cw')}</th>`:''}<th>${t('tbl.output')}</th><th>${t('tbl.sum')}</th><th>${t('tbl.share')}</th><th>${t('tbl.requests')}</th></tr>`:`<tr><th>${t('tbl.source')}</th><th>${t('tbl.requests')}</th><th>${t('tbl.input')}</th><th>${t('tbl.cr')}</th>${cw?`<th>${t('tbl.cw')}</th>`:''}<th>${t('tbl.output')}</th><th>${t('tbl.sum')}</th></tr>`;
  for(const row of page.rows){const m=row.bundle.metrics,on=selected(sel,row.id),cls=(model?'mrow':'srcrow')+(sel?(on?' solo-on':' solo-off'):'');
    const name=model?escapeHtml(row.label):`<span class="srcdot" style="background:${srcColor(row.id)}"></span>${escapeHtml(row.label)}`;
    html+=`<tr class="${cls}" role="button" tabindex="0" data-${model?'m':'s'}="${escapeHtml(row.id)}" aria-pressed="${!!sel&&on}" title="${escapeHtml(t(model?'tbl.clickModel':'tbl.clickSource'))}"><td class="mname">${name}</td>`;
    html+=model?`<td style="text-align:left">${(row.sources||[]).map(id=>`<span class="srcdot" style="background:${srcColor(id)}"></span>${escapeHtml(srcLabel(id))}`).join(' ')}</td>`:`<td>${metricHTML(m.requests,false)}</td>`;
    for(const key of ['input_uncached','cache_read',...(cw?['cache_write']:[]),'output_combined'])html+=`<td>${metricHTML(m[key])}${!model&&key==='cache_read'?sourceCacheReadShareHTML(m):''}</td>`;
    html+=`<td><b>${metricHTML(m.total)}</b></td>`;
    if(model){
      const share=shareText(row.share),bar=share==='—'?'':`<span class="share-bar" style="width:${share}"></span>`;
      html+=`<td class="share-cell" title="${escapeHtml(shareTitle(row.share))}">${bar}<span class="share-txt">${escapeHtml(share)}</span></td><td>${metricHTML(m.requests,false)}</td>`;
    }
    html+='</tr>';
  }
  if(!page.rows.length)html+=`<tr><td colspan="${(model?8:6)+(cw?1:0)}" class="empty-tip">${t(model?'empty.noSel':'empty.noRange')}</td></tr>`;
  setTableHTML('tbl-'+dim,html);
  $('#more-'+dim).hidden=!page.next_cursor;
}
function shareText(share) {
  if(share?.ratio_decimal==null)return '—';
  const [whole,fraction='']=share.ratio_decimal.split('.'),denominator=10n**BigInt(fraction.length);
  return ratioText({known_value:(BigInt(whole)*denominator+BigInt(fraction||'0')).toString()},{known_value:denominator.toString()});
}
function shareTitle(share) {
  return shareText(share);
}
async function loadTablePage(dim) {
  const view=v2View,signal=v2Controller?.signal;if(!view||view.tableBusy.has(dim)||!view.tables[dim].next_cursor)return;
  view.tableBusy.add(dim);renderDetailTable(dim);
  try{const result=await apiV2('query',{kind:'table',query:{...view.query,metric:'total'},dimension:dim,scope:'facet',page:{limit:100,cursor:view.tables[dim].next_cursor}},signal);
    if(view!==v2View||view.key!==currentQueryKey())return;verifyStamp(view.response,result);const page=result.payload.data;
    view.tables[dim]={...page,rows:[...view.tables[dim].rows,...page.rows]};registerLabels(dim,page.rows);DATA.view_generation=++viewGeneration;
  }catch(e){if(e.name!=='AbortError'&&!signal?.aborted&&view===v2View&&view.key===currentQueryKey()){if(['CURSOR_STALE','TIMEZONE_CHANGED'].includes(e.code))load(true);else showLoadFailure(e);}}
  finally{view.tableBusy.delete(dim);if(view===v2View&&view.key===currentQueryKey())renderDetailTable(dim);}
}
function renderSelectorButtons() {
  for(const dim of ['source','model']){
    const sel=dim==='source'?state.srcSel:state.mdlSel,button=$(dim==='source'?'#src-ms-btn':'#mdl-ms-btn'),ids=catalogItems(dim).map(x=>x.id);
    let label=t(dim==='source'?'ms.allSrc':'ms.allMdl'),sub=ids.length?t('ms.nItems',ids.length):'';
    if(sel){const live=ids.filter(id=>selected(sel,id));
      if(!live.length){label=t(dim==='source'?'ms.noSrc':'ms.noMdl');sub='';}
      else if(live.length===1){label=labels[dim].get(live[0])||live[0];sub=t('ms.oneItem');}
      else {label=t('ms.selected',live.length,ids.length);sub='';}
    }
    button.classList.toggle('filter-on',!!sel);button.innerHTML=`<span class="lbl">${escapeHtml(label)}</span> <span class="cnt">${sub?escapeHtml(sub)+' · ':''}▾</span>`;
  }
}
function filterWindowKey(){return JSON.stringify([rangeBounds(),v2View?.response.stamp,v2View?.response.as_of_ms]);}
function buildFilterPanel(dim) {
  if(!v2View)return;
  const element=$(dim==='source'?'#src-ms-panel':'#mdl-ms-panel'),previous=filterPanels[dim];
  const scroll=element.querySelector('.ms-list')?.scrollTop||0;
  previous?.controller.abort();
  const panel=filterPanels[dim]={element,key:filterWindowKey(),search:element.dataset.q||'',items:catalogItems(dim),next:null,stats:new Map(),statsNext:null,statsDone:false,busy:false,sequence:0,visibleLimit:previous?.visibleLimit||200,controller:new AbortController()};
  // Retain active inputs and controls across refreshes, including an unfinished IME composition.
  if(!previous)element.innerHTML=`<div class="ms-head"><input class="ms-search" placeholder="${escapeHtml(t('ms.search',catalogs[dim].total))}" value="${escapeHtml(panel.search)}"><button class="ms-act" data-act="all">${t('ms.all')}</button><button class="ms-act" data-act="none">${t('ms.none')}</button></div><div class="ms-list"></div><div class="ms-error" role="status" style="color:var(--muted);padding:0 8px"></div>`;
  const input=element.querySelector('.ms-search');input.placeholder=t('ms.search',catalogs[dim].total);
  if(!previous||element.hidden||document.activeElement!==input)input.value=panel.search;
  input.oninput=e=>{if(e.isComposing)return;panel.search=e.target.value;element.dataset.q=panel.search;panel.visibleLimit=200;renderFilterList(dim);};
  for(const act of ['all','none']){const button=element.querySelector(`[data-act="${act}"]`);button.textContent=t('ms.'+act);button.onclick=()=>{setSelection(dim,act==='all'?null:new Set(),null,true);render();};}
  const list=element.querySelector('.ms-list');
  list.onchange=e=>{const checkbox=e.target.closest('input[data-v]');if(checkbox)(dim==='source'?toggleSrc:toggleMdl)(checkbox.dataset.v);};
  list.onscroll=()=>{if(list.scrollTop+list.clientHeight>=list.scrollHeight-80&&panel.visibleLimit<panel.items.length){panel.visibleLimit+=200;renderFilterList(dim);}};
  element.querySelector('.ms-error').textContent='';
  renderFilterList(dim);list.scrollTop=scroll;
  // Only compact current-window summaries are read, when a selector is opened.
  (async()=>{await readFilterPage(dim,false);while(panel===filterPanels[dim]&&!panel.controller.signal.aborted&&panel.statsNext)await readFilterPage(dim,true);})();
}
async function readFilterPage(dim,append) {
  const panel=filterPanels[dim];if(!panel||panel.busy)return;
  const seq=++panel.sequence,view=v2View;panel.busy=true;
  try{
    const query={...view.query,sources:{mode:'all'},models:{mode:'all'},metric:'total'};
    const ranking=await apiV2('query',{kind:'table',query,dimension:dim,scope:'facet',page:{limit:200,cursor:append?panel.statsNext:null}},panel.controller.signal);
    if(panel!==filterPanels[dim]||seq!==panel.sequence)return;
    verifyStamp(view.response,ranking);
    const data=ranking.payload.data;if(!append)panel.stats.clear();
    for(const row of data.rows)panel.stats.set(row.id,row);
    panel.statsNext=data.next_cursor;panel.statsDone=!data.next_cursor;registerLabels(dim,data.rows);
    panel.element.querySelector('.ms-error').textContent='';
  }catch(e){if(panel===filterPanels[dim]&&seq===panel.sequence&&e.name!=='AbortError'){
    panel.statsNext=null;
    if(['CURSOR_STALE','TIMEZONE_CHANGED'].includes(e.code)){panel.key='';load(true);}else{
      panel.element.querySelector('.ms-error').textContent=words('用量稍后更新，筛选仍可使用。','Usage will update later; filters remain available.');
      showLoadFailure(e);
    }
  }}finally{if(panel===filterPanels[dim]&&seq===panel.sequence){panel.busy=false;renderFilterList(dim);}}
}
function renderFilterList(dim) {
  const panel=filterPanels[dim];if(!panel)return;
  const list=panel.element.querySelector('.ms-list'),scroll=list.scrollTop,focused=document.activeElement?.dataset?.v,sel=dim==='source'?state.srcSel:state.mdlSel;
  const search=panel.search.trim().toLowerCase();
  const rows=panel.items.filter(item=>!search||item.label.toLowerCase().includes(search)||item.id.toLowerCase().includes(search)).slice().sort((a,b)=>{
    const av=panel.stats.get(a.id)?.bundle.metrics.total.known_value,bv=panel.stats.get(b.id)?.bundle.metrics.total.known_value;
    if(av!=null&&bv!=null)return BigInt(av)===BigInt(bv)?0:BigInt(av)>BigInt(bv)?-1:1;
    return av!=null?-1:bv!=null?1:0;
  }).slice(0,panel.visibleLimit||200);
  list.innerHTML=rows.map(item=>{const row=panel.stats.get(item.id),label=item.label||labels[dim].get(item.id)||item.id,value=row?.bundle.metrics.total;
    const detail=value&&(value.state!=='exact'||BigInt(value.known_value||'0')>0n)?metricText(value)+' · '+shareText(row.share):'—';
    return `<label class="ms-item"><input type="checkbox" data-v="${escapeHtml(item.id)}" ${selected(sel,item.id)?'checked':''}><span class="srcdot" style="background:${dim==='source'?srcColor(item.id):mdlStableColor(item.id)}"></span><span class="lbl">${escapeHtml(label)}</span><span class="share" title="${row?escapeHtml(metricTitle(value)+' · '+shareTitle(row.share)):''}">${escapeHtml(detail)}</span></label>`;
  }).join('')||`<div class="ms-item" style="color:var(--dim);cursor:default">${t('ms.noMatch')}</div>`;
  list.scrollTop=scroll;if(focused)list.querySelector(`input[data-v="${CSS.escape(focused)}"]`)?.focus();
}
function loadVisibleTablePages() {
  if(!v2View||v2View.key!==currentQueryKey())return;
  for(const dim of ['model','source']){
    const el=$('#more-'+dim);if(el.hidden)continue;
    const box=el.getBoundingClientRect();
    if(box.top<window.innerHeight+160&&box.bottom>=0)loadTablePage(dim);
  }
}
function rescanUntilNew(done) {
  if(manualRefresh)manualRefresh.finish();
  const op={finished:false,timers:new Set(),controller:new AbortController()};manualRefresh=op;
  const active=()=>manualRefresh===op&&!op.finished;
  const later=(fn,ms)=>{const timer=setTimeout(()=>{op.timers.delete(timer);if(active())fn();},ms);op.timers.add(timer);};
  op.finish=(timedOut=false,failed=false)=>{
    if(!active())return;
    op.finished=true;for(const timer of op.timers)clearTimeout(timer);op.controller.abort();
    manualRefresh=null;rescanBusy=false;if(!failed)rescanTimedOut=timedOut;
    rescanTimedOutGen=v2Status?.refresh_generation||'0';
    $('#btn-refresh').disabled=false;$('#btn-refresh').textContent=t('btn.refresh');
    // An explicitly requested update reports incomplete sources immediately;
    // ordinary background hiccups still use the quiet notice delay.
    if(failed)scanIssueSince=Math.min(scanIssueSince??Date.now(),Date.now()-NOTICE_DELAY_MS);
    if(timedOut&&!recoveryUntil){recoveryUntil=Date.now();recoveryBase=v2Status?.refresh_generation??null;}
    if(timedOut||failed)retryInBackground();
    renderStatus();if(done)done({ok:!timedOut&&!failed,timedOut,failed});
  };
  rescanBusy=true;$('#btn-refresh').disabled=true;$('#btn-refresh').textContent=t('btn.rescaning');renderStatus();later(()=>op.finish(true),RETRY_WAIT_MS);
  const start=async()=>{
    try{
      const initial=await readStatusV2('status?arf=',op.controller.signal,active);if(!active())return;
      const base=BigInt(initial.refresh_generation||'0');
      await apiV2('refresh',{},op.controller.signal);if(!active())return;
      const poll=async()=>{try{
        const status=await readStatusV2('status?arf=',op.controller.signal,active);if(!active())return;observeScanStatus(status);renderStatus();
        if(BigInt(status.refresh_generation||'0')!==base&&!status.refreshing){
          const result=await load(true);if(!active())return;
          if(result?.ok){op.finish(false,observeScanStatus(result.status));return;}
        }
      }catch(e){if(active())showLoadFailure(e);}if(active())later(poll,RETRY_DELAY_MS);};
      later(poll,300);
    }catch(e){if(active()){showLoadFailure(e);later(start,RETRY_DELAY_MS);}}
  };start();
}

/* ---------------- 启动 ---------------- */
// 旧布局脚本已完成控件绑定与偏好恢复，这里是唯一的数据启动点。
migrateModels();
window.addEventListener('scroll',loadVisibleTablePages,{passive:true});
window.addEventListener('resize',loadVisibleTablePages);
applyArf();
setInterval(()=>{if(!DATA?.ready||document.hidden)return;if(renderedDay!==localDayStr())render();else renderStatus();},1000);
load();

'use strict';
/* 原看板的数据适配：服务端完成统计；浏览器保存轻量实体目录、当前视图和用户已展开的汇总页。 */
let v2Status = null, v2View = null, v2Controller = null, coldRetryTimer = null;
let statusReachable = null, statusRequestSeq = 0;
let statusFailureSince=null, statusNoticeTimer=null;
let wakeConfirmTimer=null,wakeGeneration=0;
let selectionIssue=null, selectionIssueDetail='', selectionIssueDimension='model', modelMigrationPending=false, validationFailureKind=null;
const selectionIssues=new Map(), tableLimitBlocks=new Map();
let viewGeneration = 0, restoringSelection = true, manualRefresh = null, pendingQueryKey = null;
let globalTotalsCache = null;
const RETRY_WAIT_MS=180000, RETRY_DELAY_MS=5000, NOTICE_DELAY_MS=60000;
let recoveryUntil=0, recoveryBase=null, loadFailureSince=null, scanIssueSince=null;
const catalogs = {source:{items:[],next:null,total:'0'},model:{items:[],next:null,total:'0'}};
const labels = {source:new Map(),model:new Map()};
const modelColorIndices = new Map();
const previousCatalogIds = {source:null,model:null};
const catalogSequences={source:0,model:0};
const catalogJobs={source:null,model:null};
let catalogTurnSignal=null;
const catalogRevisions={source:0,model:0},catalogSynced={source:0,model:0};
let catalogFailureSince=null;
const filterPanels = {source:null,model:null};
const METRIC_IDS = {total:'total',input:'input_uncached',cr:'cache_read',cw:'cache_write',output:'output_combined',reason:'reasoning_output'};
const unknownMetric = () => ({state:'unknown',known_value:null});
const words = (zh,en) => state.lang === 'en-US' ? en : zh;
const selected = (sel,id) => !sel || (sel.exclude ? !sel.has(id) : sel.has(id));
const selector = (sel,dim) => {
  if(!sel)return {mode:'all'};
  const ids=[...sel];
  return {mode:sel.exclude?(dim==='model'?'exclude_exact':'exclude'):'include',ids};
};
const catalogItems = dim => catalogs[dim].items;
const stampKey = stamp => JSON.stringify(stamp);
const queryBundleDetail = () => v2Status?.capabilities?.query_bundle_metrics===true?'metrics':'full';
const capturedBundleDetail = query => query?.bundle_detail==='metrics'?'metrics':'full';
const catalogComplete = dim => !!catalogs[dim].stamp&&!!v2Status?.stamp&&stampKey(catalogs[dim].stamp)===stampKey(v2Status.stamp);
const setScanningText = txt => { $('#scanning-txt').textContent = txt; };
function showSelectionIssue(kind,dimension='model') {
  selectionIssues.set(kind+':'+dimension,{kind,dimension});
  renderSelectionIssue();
}
function selectionIssueDescription({kind,dimension}) {
  return kind==='protocol'?words('当前服务不支持此模型筛选，请更新服务或清除筛选。','This service does not support this model filter; update it or clear the filter.'):
    kind==='storage'?words('筛选偏好未保存。','Filter preference was not saved.'):
    kind==='catalog'?(dimension==='source'?words('来源目录暂时无法读取，保留当前筛选。','The source directory cannot be read; the current filter is retained.'):words('模型目录暂时无法读取，保留当前筛选。','The model directory cannot be read; the current filter is retained.')):
    ['query','table','panel'].includes(kind)?words('当前查询受到限制，请稍后重试。','The current query is limited; please try again later.'):
    words('筛选超出限制，请清除筛选后重新选择。','Filter exceeds the limit; clear the filter and select again.');
}
function renderSelectionIssue() {
  const issues=[...selectionIssues.values()];
  const issue=issues.find(x=>x.kind==='storage')||issues.find(x=>x.kind==='catalog')||issues.find(x=>x.kind==='protocol'||x.kind==='limit')||issues[issues.length-1];
  if(!issue){selectionIssue=null;selectionIssueDetail='';$('#clear-model-filter').hidden=true;if(DATA?.ready)renderStatus();return;}
  const {kind}=issue,action=issues.find(x=>x.kind==='protocol'||x.kind==='limit');
  selectionIssue=kind;selectionIssueDetail=selectionIssueDescription(issue);
  selectionIssueDimension=(action||issue).dimension;
  const button=$('#clear-model-filter');button.hidden=!action;
  button.textContent=selectionIssueDimension==='source'?words('清除来源筛选','Clear source filter'):words('清除模型筛选','Clear model filter');
  button.title=selectionIssueDescription(action||issue);
  const text=words(kind==='catalog'?'目录受限':['query','table','panel'].includes(kind)?'查询受限':'筛选受限',kind==='catalog'?'Directory limited':['query','table','panel'].includes(kind)?'Query limited':'Filter limited');
  if(DATA?.ready)renderStatus();else{$('#status').textContent=text;$('#status').title=selectionIssueDetail;setScanningText(text);}
}
function clearSelectionIssue(kind,dimension){
  for(const [key,issue] of selectionIssues)if((!kind||issue.kind===kind)&&(!dimension||issue.dimension===dimension))selectionIssues.delete(key);
  renderSelectionIssue();
}
function validateSelection(dim,sel) {
  validationFailureKind=null;
  if(!v2Status)return true;
  const caps=v2Status.capabilities||{},model=dim==='model';
  if(model&&sel?.exclude&&caps.model_selector_exact!==true){showSelectionIssue('protocol',dim);return false;}
  const maxIds=model?(caps.model_selector_exact===true?Math.min(20000,Number(caps.model_selector_max_ids)||10000):10000):128;
  const maxIdBytes=model?1400:256;
  if(sel){
    let idBytes=0;
    for(const id of sel){const size=new TextEncoder().encode(id).length;if(size>maxIdBytes){showSelectionIssue('limit',dim);return false;}idBytes+=size;}
    const threshold=Number(caps.model_selector_large_threshold)||10000;
    const largeMax=Number(caps.model_selector_large_max_ids_bytes)||2097152;
    if(sel.size>maxIds||(model&&sel.size>threshold&&idBytes>largeMax)){showSelectionIssue('limit',dim);return false;}
  }
  const overrides=dim==='model'?{model:sel}:{source:sel};
  const body=JSON.stringify(dashboardRequest(buildQuery(null,overrides)));
  const maxBody=Math.min(16*1024*1024,Number(caps.max_body_bytes)||16*1024*1024);
  if(new TextEncoder().encode(body).length>maxBody){
    // The rest of the request may already exceed the limit. Only attribute it
    // to this selector if removing this dimension makes that request fit.
    const without=JSON.stringify(dashboardRequest(buildQuery(null,dim==='model'?{model:null}:{source:null})));
    if(sel&&new TextEncoder().encode(without).length<=maxBody)showSelectionIssue('limit',dim);
    else {validationFailureKind='query';showSelectionIssue('query');}
    return false;
  }
  return true;
}

// Only chart geometry becomes Number. Text, tooltips, totals and ratios use decimal strings/BigInt.
const integerFormatter = new Intl.NumberFormat('en-US');
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
  return integerFormatter.format(n);
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
  const issue=status.refresh_error||status.storage?.error||(status.sources||[]).some(s=>s.error&&s.error!=='LOADING');
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
function cancelWakeConfirmation() {
  wakeGeneration++;clearTimeout(wakeConfirmTimer);wakeConfirmTimer=null;
}
function resumeDashboard() {
  cancelWakeConfirmation();
  // Keep an existing recovery's generation: another wake must not make a
  // scan that just completed look like a new baseline.
  if(!recoveryUntil){recoveryUntil=Date.now()+RETRY_WAIT_MS;recoveryBase=v2Status?.refresh_generation??null;}
  if(DATA?.ready){$('#ledger-note').hidden=true;renderStatus();}
  return load(true);
}

async function apiV2(path,body,signal) {
  const controller=new AbortController();
  const abort=()=>controller.abort();
  if(signal?.aborted)controller.abort();else signal?.addEventListener('abort',abort,{once:true});
  let timeout;
  try {
    const encoded=body?JSON.stringify(body):undefined;
    const maxBody=Math.min(16*1024*1024,Number(v2Status?.capabilities?.max_body_bytes)||16*1024*1024);
    if(encoded&&new TextEncoder().encode(encoded).length>maxBody){const error=new Error('Request exceeds limit');error.code='QUERY_LIMIT';throw error;}
    return await Promise.race([
      (async()=>{
        const response=await fetch('/api/v2/'+path,{method:body?'POST':'GET',headers:body?{'Content-Type':'application/json'}:{},body:encoded,cache:'no-store',signal:controller.signal});
        let value;try{value=await response.json();}catch(e){if(e.name==='AbortError')throw e;const error=new Error(`HTTP ${response.status}`);error.httpStatus=response.status;throw error;}
        if(!response.ok){const detail=typeof value?.error==='object'?value.error:{code:value?.error,message:value?.message};const e=new Error(detail?.message||`HTTP ${response.status}`);e.code=detail?.code;e.httpStatus=response.status;throw e;}
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
let chartIntentRevision=0;
const doughnutHiddenIds=new Set();
// Each bar chart owns a bounded history. Only hidden identities need storage;
// namespace separates models, sources and the ungrouped aggregate. Other is a
// stable bucket identity, independent of whichever models currently compose it.
const BAR_HIDDEN_LIMIT=512;
const barVisibility={
  '#chartDaily':{hidden:new Map(),seen:0},
  '#chartHourly':{hidden:new Map(),seen:0},
};
const barSeriesKey=(namespace,id)=>JSON.stringify([namespace,id]);
function queryIntentKey() {
  return JSON.stringify([state.win,state.win==='custom'?[state.rs,state.re]:null,_selKey(state.srcSel),_selKey(state.mdlSel),state.metric,state.group]);
}
function resetChartIntent(){
  chartIntentRevision++;doughnutHiddenIds.clear();
  for(const entry of Object.values(barVisibility)){entry.hidden.clear();entry.seen=0;}
}
function changeQueryIntent(change){const before=queryIntentKey();change();if(before!==queryIntentKey())resetChartIntent();}
function rememberDoughnutVisibility(chart) {
  if(!chart?.$entityIds||chart.$intentRevision!==chartIntentRevision)return;
  chart.$entityIds.forEach((id,index)=>{if(chart.getDataVisibility(index))doughnutHiddenIds.delete(id);else doughnutHiddenIds.add(id);});
}
function rememberBarVisibility(id,chart) {
  if(!chart?.$seriesIds||chart!==charts[id]||chart.$intentRevision!==chartIntentRevision)return;
  const entry=barVisibility[id];
  chart.$seriesIds.forEach((entity,index)=>{
    const key=barSeriesKey(chart.$seriesNamespace,entity);
    if(chart.isDatasetVisible(index))entry.hidden.delete(key);
    else if(!entry.hidden.has(key))entry.hidden.set(key,entry.seen);
  });
  trimBarVisibility(entry,new Set(chart.$seriesIds.map(entity=>barSeriesKey(chart.$seriesNamespace,entity))));
}
function trimBarVisibility(entry,current) {
  // Server charts have a bounded current key set (Top-N or registered sources).
  // Keep those choices; evict the least recently displayed absent identities.
  if(entry.hidden.size<=BAR_HIDDEN_LIMIT)return;
  const absent=[...entry.hidden].filter(([key])=>!current.has(key)).sort((a,b)=>a[1]-b[1]);
  for(const [key] of absent){if(entry.hidden.size<=BAR_HIDDEN_LIMIT)break;entry.hidden.delete(key);}
}
function hiddenBarSeries(id,namespace,ids) {
  rememberBarVisibility(id,charts[id]);
  const entry=barVisibility[id],current=new Set(ids.map(entity=>barSeriesKey(namespace,entity)));
  entry.seen++;
  for(const key of current)if(entry.hidden.has(key))entry.hidden.set(key,entry.seen);
  trimBarVisibility(entry,current);
  return new Set(ids.filter(entity=>entry.hidden.has(barSeriesKey(namespace,entity))));
}
function currentQueryKey() {
  return JSON.stringify([state.win,state.rs,state.re,localDayStr(),_selKey(state.srcSel),_selKey(state.mdlSel),state.metric,state.group,v2Status?.stamp?.report_timezone,queryBundleDetail()]);
}
function buildQuery(windowOverride,overrides={}) {
  const bounds=rangeBounds();
  return {window:windowOverride||(bounds?{kind:'calendar',start_date:bounds[0],end_date_exclusive:addDay(bounds[1],1)}:{kind:'all'}),report_timezone:v2Status.stamp.report_timezone,as_of_ms:null,sources:selector(Object.hasOwn(overrides,'source')?overrides.source:state.srcSel,'source'),models:selector(Object.hasOwn(overrides,'model')?overrides.model:state.mdlSel,'model'),metric:METRIC_IDS[state.metric],group_by:state.group,...(queryBundleDetail()==='metrics'?{bundle_detail:'metrics'}:{})};
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
async function readFullCatalog(dim,signal,expectedStamp=null) {
  const sequence=++catalogSequences[dim];
  // Selection recovery must only see complete directories, even after a failed page.
  for(let attempt=0;attempt<2;attempt++){
    try{
      const items=[];let first=null,cursor=null;
      do{
        const page=await apiV2('catalog',{dimension:dim,search:'',page:{limit:200,cursor}},signal);
        if(signal?.aborted||sequence!==catalogSequences[dim])return false;
        if(expectedStamp&&stampKey(page.stamp)!==stampKey(expectedStamp)){const e=new Error('Catalog snapshot changed');e.code='CURSOR_STALE';throw e;}
        if(first)verifyStamp(first,page);else first=page;
        items.push(...page.items);cursor=page.next_cursor;
      }while(cursor);
      registerLabels(dim,items);
      catalogs[dim]={items,next:null,total:first.total_count,stamp:first.stamp};
      catalogRevisions[dim]++;
      return true;
    }catch(e){
      if(signal?.aborted||sequence!==catalogSequences[dim])return false;
      if(attempt===0&&['CURSOR_STALE','TIMEZONE_CHANGED'].includes(e.code))continue;
      throw e;
    }
  }
}
function catalogTask(dim,stamp) {
  const key=stampKey(stamp),old=catalogJobs[dim];
  // Source membership also depends on the time cutoff chosen on its first
  // page. Start a fresh source task per read; model membership is stamp keyed.
  if(dim==='model'&&old&&old.key===key&&!old.controller.signal.aborted)return old.promise;
  old?.controller.abort();
  const controller=new AbortController(),job={key,controller,promise:null};
  job.promise=readFullCatalog(dim,controller.signal,stamp).finally(()=>{if(catalogJobs[dim]===job)catalogJobs[dim]=null;});
  catalogJobs[dim]=job;return job.promise;
}
function catalogReadTurn(signal,isCurrent) {
  // Do not enqueue SQL for directories that a rapid next click will discard.
  // Once dispatched, aborting fetch cannot guarantee server work is stopped.
  return new Promise(resolve=>{
    let timer;
    const finish=active=>{signal.removeEventListener('abort',cancel);if(catalogTurnSignal===signal)catalogTurnSignal=null;resolve(active);};
    const cancel=()=>{clearTimeout(timer);finish(false);};
    if(signal.aborted){resolve(false);return;}
    catalogTurnSignal=signal;
    signal.addEventListener('abort',cancel,{once:true});
    timer=setTimeout(()=>finish(!signal.aborted&&isCurrent()),150);
  });
}
function syncSelections(dimensions=['source','model']) {
  for(const dim of dimensions){
    const key=dim==='source'?'srcSel':'mdlSel',ids=catalogItems(dim).map(x=>x.id),previous=previousCatalogIds[dim];
    let sel=state[key],solo=state[dim==='source'?'srcSolo':'mdlSolo'];
    // Stable model IDs contain case-sensitive base64url; only source IDs have legacy case aliases.
    const matchKey=id=>dim==='source'?id.toLowerCase():id;
    if(!catalogComplete(dim)||catalogs[dim].next!==null||!ids.every(id=>typeof id==='string'))return false;
    const oldSel=sel,oldSolo=solo;
    if(solo!==null){
      const match=ids.find(id=>matchKey(id)===matchKey(solo));
      // Source catalogs depend on the cutoff and successful source scans, so even
      // a complete first directory cannot prove that a saved target is invalid.
      // Models are invalidated only by the global server check after the query.
      solo=match??solo;sel=new Set([solo]);
    }else if(sel&&ids.length&&!sel.exclude){
      if(sel.size){
        const known=new Map(ids.map(id=>[matchKey(id),id]));
        // A directory can omit a selected source at its cutoff, or a model
        // selected from a newer table while the directory is still catching up.
        const alive=new Set([...sel].map(id=>known.get(matchKey(id))??id));
        if(previous)for(const id of ids)if(!previous.has(matchKey(id)))alive.add(id);
        sel=alive;
      }
    }
    // Do not advance the baseline when the proposed selection cannot be used.
    if(!validateSelection(dim,sel))return false;
    if((_selKey(sel)!==_selKey(oldSel)||solo!==oldSolo)&&!setSelection(dim,sel,solo))return false;
    const seen=new Set(previous||[]);
    for(const id of ids)seen.add(matchKey(id));
    previousCatalogIds[dim]=seen;
  }
  return true;
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
  state.mdlSel=next;state.mdlSolo=state.mdlSolo===null?null:migrate(state.mdlSolo);modelMigrationPending=true;
}
function dashboardRequest(query){
  const trend=state.win.includes('week')?{unit:'week',periods:10,anchor_date:localDayStr()}:state.win.includes('month')?{unit:'month',periods:12,anchor_date:localDayStr()}:null;
  return {kind:'dashboard',query,series_unit:'day',max_points:10000,table_limit:100,comparison:comparisonSpec(),trend};
}
async function fetchDashboard(signal) {
  const query=buildQuery(),key=currentQueryKey();
  const response=await apiV2('query',dashboardRequest(query),signal);
  const payload=response.payload.data, fixed={...query,as_of_ms:response.as_of_ms}, extra=[];
  let day=payload.selected_day??payload.latest_day,sourceTable=payload.source_table,modelTable=payload.model_table;
  // Future-dated facts and daily evidence can become eligible as time advances
  // without changing the data stamp. Totals must share this query's cutoff.
  const globalKey=stampKey(response.stamp)+'|'+response.as_of_ms+'|'+capturedBundleDetail(query);
  const isGlobal=query.window.kind==='all'&&query.sources.mode==='all'&&query.models.mode==='all';
  let globalMetrics=payload.global_metrics??(isGlobal?payload.totals.metrics:globalTotalsCache?.key===globalKey?globalTotalsCache.metrics:null);
  if((payload.global_metrics||isGlobal)&&!signal?.aborted)globalTotalsCache={key:globalKey,metrics:globalMetrics};
  if(!globalMetrics)extra.push(apiV2('query',{kind:'totals',query:{...fixed,window:{kind:'all'},sources:{mode:'all'},models:{mode:'all'},group_by:'none',metric:'total'}},signal).then(r=>{
    verifyStamp(response,r);globalMetrics=r.payload.data.metrics;
    if(!signal?.aborted)globalTotalsCache={key:globalKey,metrics:globalMetrics};
  }));
  if(isDayWin(state.win)&&day?.date!==winDay()){
    const date=winDay();extra.push(apiV2('query',{kind:'day',query:{...fixed,window:{kind:'calendar',start_date:date,end_date_exclusive:addDay(date,1)}},date},signal).then(r=>{verifyStamp(response,r);day=r.payload.data;}));
  }
  // Detail rows always rank by total tokens, independent of the selected chart metric.
  if(query.metric!=='total'&&payload.detail_tables_metric!=='total')for(const dimension of ['source','model'])extra.push(apiV2('query',{kind:'table',query:{...fixed,metric:'total'},dimension,scope:'facet',page:{limit:100,cursor:null}},signal).then(r=>{verifyStamp(response,r);if(dimension==='source')sourceTable=r.payload.data;else modelTable=r.payload.data;}));
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
async function load(passive,signal) {
  clearTimeout(wakeConfirmTimer);wakeConfirmTimer=null;
  const seq=++loadSeq,intentRevision=chartIntentRevision;pendingQueryKey=currentQueryKey();v2Controller?.abort();const controller=v2Controller=new AbortController();
  if(v2View&&v2View.key!==pendingQueryKey)for(const dim of ['source','model']){
    cancelFilterRead(dim);catalogJobs[dim]?.controller.abort();
  }
  const abort=()=>controller.abort();
  if(signal?.aborted)controller.abort();else signal?.addEventListener('abort',abort,{once:true});
  clearTimeout(coldRetryTimer);coldRetryTimer=null;
  try {
    if(DATA?.ready)renderStatus();
    let status=await readStatusV2('dashboard/status?arf='+(passive===true?'':state.arf),controller.signal,()=>seq===loadSeq);
    if(seq!==loadSeq||controller.signal.aborted)return {ok:false,superseded:true};
    let scanIssue=observeScanStatus(status);
    registerLabels('source',(status.sources||[]).map(s=>({id:s.id,label:s.label||s.id})));
    if(!status.ready){
      loadFailureSince=null;loadFailed=false;
      if(!DATA){
        $('#app').hidden=true;$('#scanning').hidden=false;setScanningText(status.refresh_error?t('status.scanFailRetry'):t('data.loading'));
        coldRetryTimer=setTimeout(()=>{if(!document.hidden)load(true);},status.refresh_error?5000:3000);
      }else{
        renderStatus();if(!recoveryUntil){recoveryUntil=Date.now()+RETRY_WAIT_MS;recoveryBase=status.refresh_generation;}
        retryInBackground();
      }
      return {ok:false,pending:true};
    }
    if(!validateSelection('model',state.mdlSel)||!validateSelection('source',state.srcSel))return validationFailureKind==='query'?{ok:false,queryLimited:true}:{ok:false,selectionLimited:true};
    pendingQueryKey=currentQueryKey();
    let next;
    for(let attempt=0;attempt<2;attempt++){
      try{next=await fetchDashboard(controller.signal);break;}
      catch(e){if(attempt===0&&['CURSOR_STALE','TIMEZONE_CHANGED'].includes(e.code)){status=await readStatusV2('dashboard/status?arf=',controller.signal,()=>seq===loadSeq);scanIssue=observeScanStatus(status);continue;}throw e;}
    }
    if(seq!==loadSeq||controller.signal.aborted||intentRevision!==chartIntentRevision||next.key!==currentQueryKey())return {ok:false,superseded:true};
    let selectionConfirmationDeferred=false;
    if(restoringSelection){
      // UNKNOWN_SOURCE uses the static source registry; directory absence alone is insufficient.
      const clearSource=state.srcSolo!==null&&next.response.warnings.includes('UNKNOWN_SOURCE:'+state.srcSolo);
      let clearModel=false;
      if(state.mdlSel&&!state.mdlSel.exclude&&state.mdlSel.size&&next.response.warnings.includes('ALL_SELECTED_MODELS_UNKNOWN')){
        // A failed source can hide its models, even when that source is not selected.
        // Check all sources at this snapshot/cutoff without reusing cached coverage.
        let confirmation;
        try{
          confirmation=await apiV2('query',{kind:'totals',query:{...next.query,window:{kind:'all'},sources:{mode:'all'},metric:'total',group_by:'none'}},controller.signal);
          verifyStamp(next.response,confirmation);
        }catch(e){
          if(e.code!=='CURSOR_STALE')throw e;
          // This check may clear restored preferences; it supplies no displayed
          // values. Keep the selection and render the consistent dashboard.
          confirmation=null;selectionConfirmationDeferred=true;
        }
        if(seq!==loadSeq||controller.signal.aborted||intentRevision!==chartIntentRevision||next.key!==currentQueryKey())return {ok:false,superseded:true};
        clearModel=confirmation?.source_coverage==='complete'&&confirmation.warnings.includes('ALL_SELECTED_MODELS_UNKNOWN');
      }
      if(clearSource||clearModel){
        restoringSelection=selectionConfirmationDeferred;
        if(clearSource)setSelection('source',null);
        if(clearModel)setSelection('model',null);
        return load(true,signal);
      }
    }
    restoringSelection=selectionConfirmationDeferred;v2View=next;loadFailed=false;loadFailureSince=null;
    if(modelMigrationPending){modelMigrationPending=false;if(!persist(MDL_LS_KEY,state.mdlSel,state.mdlSolo))showSelectionIssue('storage','model');else clearSelectionIssue('storage','model');}
    clearSelectionIssue('protocol');clearSelectionIssue('limit');clearSelectionIssue('query');
    for(const [dim,block] of tableLimitBlocks){
      if(block.key!==next.key||block.stamp!==stampKey(next.response.stamp)||!next.tables[dim].next_cursor){
        tableLimitBlocks.delete(dim);clearSelectionIssue('table',dim);
      }
    }
    registerLabels('source',next.tables.source.rows);registerLabels('model',next.tables.model.rows);
    if(state.metric==='cw'&&!cwVisible()){state.metric='total';segSync('#seg-metric',v=>v==='total');return load(true,signal);}
    DATA={ready:true,generated_at_ms:Number(status.generated_at_ms||next.response.as_of_ms),view_generation:++viewGeneration,last_scan_failed:!!status.refresh_error,version:status.version,sources:status.sources||[]};
    refreshTimedOut=false;
    $('#scanning').hidden=true;$('#app').hidden=false;
    prepareDays();next.key=currentQueryKey();renderPayload();
    // Give the selected dashboard the query workers before directory reads.
    // Source membership is as-of dependent; model names are snapshot metadata.
    if(catalogJobs.model&&catalogJobs.model.key!==stampKey(status.stamp))catalogJobs.model.controller.abort();
    const changedCatalogs=['source','model'].filter(dim=>dim==='source'||!catalogs[dim].stamp||stampKey(catalogs[dim].stamp)!==stampKey(status.stamp));
    const catalogTurn=catalogReadTurn(controller.signal,()=>seq===loadSeq&&intentRevision===chartIntentRevision);
    const catalogWork=changedCatalogs.map(dim=>({dim,promise:catalogTurn.then(active=>active?catalogTask(dim,status.stamp):false).then(ok=>({ok}),error=>({error}))}));
    if(!recoveryUntil&&(status.refreshing||scanIssue)){recoveryUntil=Date.now()+RETRY_WAIT_MS;recoveryBase=status.refresh_generation;}
    if(recoveryUntil){
      if(!status.refreshing&&!scanIssue&&(recoveryBase===null||status.refresh_generation!==recoveryBase)){recoveryUntil=0;recoveryBase=null;clearTimeout(coldRetryTimer);coldRetryTimer=null;}
      else retryInBackground();
    }
    const syncCatalog=dim=>{
      if(seq!==loadSeq||controller.signal.aborted||intentRevision!==chartIntentRevision||
        catalogSynced[dim]===catalogRevisions[dim]||stampKey(catalogs[dim].stamp)!==stampKey(status.stamp))return;
      const before=currentQueryKey();if(!syncSelections([dim]))return;catalogSynced[dim]=catalogRevisions[dim];
      if(currentQueryKey()!==before){load(true);return;}
      renderSelectorButtons();
      syncFilterCatalog(dim);
    };
    // A complete model directory can be displayed while an unrelated source
    // directory is still loading, without changing the saved selection yet.
    for(const {dim,promise} of catalogWork)void promise.then(({ok})=>{
      if(!ok||seq!==loadSeq||controller.signal.aborted||intentRevision!==chartIntentRevision||stampKey(catalogs[dim].stamp)!==stampKey(v2Status?.stamp))return;
      clearSelectionIssue('catalog',dim);
      if(dim!=='model')return;
      renderSelectorButtons();
      syncFilterCatalog('model');
    });
    void Promise.all(catalogWork.map(work=>work.promise)).then(results=>{
      if(seq!==loadSeq||controller.signal.aborted||intentRevision!==chartIntentRevision)return;
      for(const dim of ['source','model'])syncCatalog(dim);
      for(let i=0;i<results.length;i++)if(['QUERY_LIMIT','BODY_TOO_LARGE'].includes(results[i].error?.code))showSelectionIssue('catalog',catalogWork[i].dim);
      const failed=results.find(result=>result.error&&result.error.name!=='AbortError'&&!['QUERY_LIMIT','BODY_TOO_LARGE'].includes(result.error.code));
      if(failed){catalogFailureSince??=Date.now();loadFailureSince=catalogFailureSince;showLoadFailure(failed.error);retryInBackground();}
      else catalogFailureSince=null;
    });
    return {ok:true,status};
  } catch(e){
    if(seq!==loadSeq||controller.signal.aborted||e.name==='AbortError')return {ok:false,superseded:true};
    if(e.code==='INVALID_CUSTOM_RANGE')return {ok:false,invalidRange:true};
    if(['QUERY_LIMIT','BODY_TOO_LARGE'].includes(e.code)){showSelectionIssue('query');return {ok:false,queryLimited:true};}
    showLoadFailure(e);if(!DATA)coldRetryTimer=setTimeout(()=>{if(!document.hidden)load(true);},5000);
    return {ok:false};
  }
  finally {signal?.removeEventListener('abort',abort);if(seq===loadSeq){pendingQueryKey=null;if(DATA?.ready)renderStatus();}}
}
function render() {
  setWinSegUi();renderSelectorButtons();
  if(v2View&&v2View.key===currentQueryKey()){
    renderPayload();
    // Returning to a displayed range may have cancelled its cold directory
    // before dispatch. Recover even when the intervening query already failed.
    const missingCatalog=['source','model'].some(dim=>!catalogComplete(dim)&&
      (!catalogTurnSignal||catalogTurnSignal.aborted)&&(!catalogJobs[dim]||catalogJobs[dim].controller.signal.aborted));
    if(pendingQueryKey!==currentQueryKey()&&(pendingQueryKey!==null||missingCatalog))load(true);
    return;
  }
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
  renderCards();renderStatus();
  try{renderCharts();}catch(error){console.error('Chart rendering failed',error);}
  renderModelTable();renderSourceTable();
  if(!loadFailed){$('#ledger-note').textContent='';$('#ledger-note').hidden=true;}
  for(const dimension of ['source','model']){
    const panel=filterPanels[dimension];if(panel&&!panel.element.hidden){
      if(panel.controller.signal.aborted||panel.key!==filterWindowKey())buildFilterPanel(dimension);else renderFilterList(dimension);
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
function invalidateBundleDetail(previous,signal) {
  if(previous===queryBundleDetail())return;
  globalTotalsCache=null;tableLimitBlocks.clear();
  for(const dim of ['source','model']){
    selectionIssues.delete('table:'+dim);selectionIssues.delete('panel:'+dim);
    cancelFilterRead(dim);catalogJobs[dim]?.controller.abort();
    const panel=filterPanels[dim];
    if(panel){panel.key='';panel.stats.clear();panel.statsNext=null;panel.statsDone=false;renderFilterList(dim);}
    // Keep the previous values visible during recovery, but their cursor
    // belongs to a different request contract and must never be resumed.
    if(v2View&&capturedBundleDetail(v2View.query)!==queryBundleDetail())v2View.tables[dim].next_cursor=null;
    $('#more-'+dim).hidden=true;
  }
  renderSelectionIssue();
  // The owning load has not built its query yet and can continue with the
  // new capability. An independent status read (including arf=0) must
  // replace any old load, page reads and filter work with a fresh first page.
  if((!signal||signal!==v2Controller?.signal)&&(v2View||pendingQueryKey!==null)){
    v2Controller?.abort();void load(true);
  }
}
async function readStatusV2(path,signal,isCurrent) {
  const sequence=++statusRequestSeq;
  const current=()=>sequence===statusRequestSeq&&!signal?.aborted&&isCurrent();
  try {
    let result;
    try{result=await apiV2(path,undefined,signal);}
    catch(error){
      if(error.httpStatus!==404||!/^dashboard\/status(?:\?|$)/.test(path)||!current())throw error;
      result=await apiV2(path.replace(/^dashboard\//,''),undefined,signal);
    }
    if(!result||typeof result!=='object'||!Array.isArray(result.sources))throw new Error('Invalid status response');
    if(current()){
      const previous=queryBundleDetail();
      v2Status=result;statusReachable=true;statusFailureSince=null;
      clearTimeout(statusNoticeTimer);statusNoticeTimer=null;
      invalidateBundleDetail(previous,signal);
      if(DATA?.ready)renderStatus();
    }
    return result;
  } catch(error) {
    if(current()&&error?.name!=='AbortError'){
      // An explicit server limit is a valid response, not a lost connection.
      if(['QUERY_LIMIT','BODY_TOO_LARGE'].includes(error.code)){
        statusReachable=true;statusFailureSince=null;clearTimeout(statusNoticeTimer);statusNoticeTimer=null;
        if(DATA?.ready)renderStatus();
        throw error;
      }
      statusReachable=false;statusFailureSince??=Date.now();
      clearTimeout(statusNoticeTimer);
      if(!signal)statusNoticeTimer=setTimeout(()=>{statusNoticeTimer=null;if(statusReachable===false)renderStatus();},Math.max(0,NOTICE_DELAY_MS-(Date.now()-statusFailureSince)));
      if(DATA?.ready)renderStatus();
    }
    throw error;
  }
}
function renderStatus() {
  if(!DATA?.ready)return;
  const asOf=t('status.dataAsOf',localDate(DATA.generated_at_ms)),stale=localDayStr(new Date(DATA.generated_at_ms))!==localDayStr();
  const details=[asOf];
  if(selectionIssue)details.push(selectionIssueDetail);
  const scanNotice=scanIssueSince!==null&&Date.now()-scanIssueSince>=NOTICE_DELAY_MS;
  const connectionFailed=statusReachable===false&&statusFailureSince!==null&&Date.now()-statusFailureSince>=NOTICE_DELAY_MS;
  const captureSaveFailure=typeof v2Status?.refresh_error==='string'&&v2Status.refresh_error.startsWith('CAPTURE_SAVE_FAILED:');
  if(connectionFailed)details.push(t('status.connFail'));
  if(loadFailed||connectionFailed)details.push(t('st.connFailShow'));else if(v2Status?.refresh_error&&!captureSaveFailure&&scanNotice)details.push(t('st.scanFailShow'));
  if(stale)details.push(t('st.waitData'));
  const key=currentQueryKey(),mismatched=!!(v2View&&v2View.key!==key);
  const reading=pendingQueryKey===key,pending=mismatched&&reading;
  if(pending)details.push(words('正在读取所选范围…','Loading selected range…'));
  else if(mismatched)details.push(words('当前选择尚未更新，显示此前统计。','The current selection has not updated; showing previous results.'));
  const failed=(v2Status?.sources||[]).filter(s=>s.error&&s.error!=='LOADING').length;
  if(failed&&scanNotice)details.push(t('st.srcFailed',failed));
  if(refreshTimedOut)details.push(t('st.refreshTimeout'));
  if(v2Status?.refreshing)details.push(t('st.bgScan'));
  if((reading||refreshBusy)&&!state.arf)details.push(t('st.readingSaved'));
  const isolated=(v2Status?.sources||[]).reduce((n,s)=>n+BigInt(s.isolated_objects||'0'),0n);
  if(isolated)details.push(words(`${isolated} 个文件已隔离`,`${isolated} files isolated`));
  const pendingHistory=(v2Status?.sources||[]).reduce((n,s)=>n+BigInt(s.pending_compatibility_observations||'0'),0n);
  if(pendingHistory)details.push(words(`${pendingHistory} 条历史用量待核对，尚未计入新增。`,`${pendingHistory} historical usage observations need review and are not included as new usage.`));
  for(const source of v2Status?.sources||[])if(source.accounting?.state==='partial'){
    const count=source.accounting.unallocated_sessions||'0';
    details.push(words(`${srcLabel(source.id)}：${count} 个会话存在未能归入请求的累计用量。`,
      `${srcLabel(source.id)}: cumulative usage in ${count} sessions cannot be fully attributed to requests.`));
  }
  const remaining=Math.max(0,Math.ceil((arfNextAt-Date.now())/1000));
  const refreshDetail=state.arf?(reading||refreshBusy?t('st.arfReading',state.arf):t('st.arf',state.arf,remaining)):t('st.arfOff');
  details.push(refreshDetail);
  // Keep the ticking digits outside the live status message and reserve their space.
  const countdown=$('#refresh-countdown');
  countdown.hidden=!state.arf||reading||refreshBusy;
  countdown.textContent=countdown.hidden?'':t('st.nextRefresh',remaining);
  countdown.title=refreshDetail;
  countdown.setAttribute('aria-label',refreshDetail);
  const updating=!!(reading||v2Status?.refreshing||refreshBusy);
  const saved=loadFailed||connectionFailed||refreshTimedOut||stale||(mismatched&&!pending)||isolated>0n||(scanNotice&&(v2Status?.refresh_error||failed));
  const persistenceFailure=(v2Status?.sources||[]).find(s=>['save_error','publish_error','recovery_unverified'].includes(s.state));
  const storageFailure=v2Status?.storage?.error;
  const failureText=storageFailure||captureSaveFailure||persistenceFailure?.state==='save_error'?words('保存失败','Save failed'):persistenceFailure?.state==='publish_error'?words('入账失败','Publish failed'):persistenceFailure?words('待恢复','Recovery pending'):null;
  const el=$('#status'),text=failureText||(selectionIssue?words(selectionIssue==='storage'?'未保存':selectionIssue==='catalog'?'目录受限':['query','table','panel'].includes(selectionIssue)?'查询受限':'筛选受限',selectionIssue==='storage'?'Not saved':selectionIssue==='catalog'?'Directory limited':['query','table','panel'].includes(selectionIssue)?'Query limited':'Filter limited'):pendingHistory?words('待核对','Review pending'):t(updating?'st.shortUpdating':saved?'st.shortSaved':'st.shortReady'));
  if(el.dataset.last!==text){el.textContent=text;el.dataset.last=text;}
  el.classList.toggle('st-scan',updating);
  el.title=[...new Set([...details,...dataNotes(),...(v2Status?.sources||[]).filter(s=>s.error).map(s=>`${srcLabel(s.id)}: ${s.error}`),v2Status?.refresh_error||'',v2Status?.storage?.error||''].filter(Boolean))].join('\n');
  const header=document.querySelector('header h1');if(header)header.title=t('status.dataAsOf',localDate(DATA.generated_at_ms));
  if(DATA.version)$('#ver').textContent=t('ver.note',DATA.version);
}
function dataNotes() {
  if(!v2View)return [];
  const r=v2View.response,d=r.payload.data,notes=[];
  if(r.source_coverage==='partial')notes.push(words('当前统计未覆盖全部来源用量。','Totals do not cover all source usage.'));
  if(r.warnings.includes('LIVE_HISTORY_UNPROTECTED'))notes.push(words('部分来源仍依赖原始历史文件。','Some sources still depend on their original history files.'));
  if(d.series?.grouping_unallocated?.known_value && d.series.grouping_unallocated.known_value!=='0')notes.push(words('部分历史汇总跨越显示日期，仍计入总量，但未拆分到日图。','Some historical aggregates cross display dates; totals retain them without splitting them across daily bars.'));
  if(r.warnings.includes('MEMBERSHIP_UNKNOWN'))notes.push(words('部分记录无法确定是否属于所选范围，已单独保留。','Some records have uncertain filter membership and are kept separate.'));
  if(d.totals.quality?.source_reported_total?.known_value && d.totals.quality.source_reported_total.known_value!=='0')notes.push(words('包含按来源报告采用的固定历史用量，尚未逐请求核实。','Includes frozen source-reported history without per-request verification.'));
  if(r.warnings.includes('DAY_EVIDENCE_EXCLUDED'))notes.push(words('有时区或覆盖范围待确认的日汇总，暂未计入。','Daily records with unverified timezone or coverage are excluded.'));
  if(r.warnings.includes('SESSION_USAGE_UNALLOCATED'))notes.push(words('部分会话累计无法完整归入请求；图表与排行只包含可归属的用量。','Some session totals cannot be fully attributed to requests; charts and rankings include attributable usage only.'));
  if(d.totals.coverage.grain==='day'||d.totals.coverage.grain==='mixed')notes.push(words('日汇总计入总量；小时图仅展示真实的小时记录。','Totals include daily records; hourly charts show only known hourly usage.'));
  if(d.series.compressed)notes.push(words('时间跨度较长，主图已按周或月合并显示。','The long date range is grouped by week or month.'));
  return notes;
}
function displayKey(key){return key.id==='__other__'?t('bucket.other'):key.label;}
function chartColor(key,index){return state.group==='source'?srcColor(key.id):key.id==='__other__'?'#8b949e':mdlStableColor(key.id);}
function drawSeries(id,series,onClick,presentation={}) {
  const keys=series?.keys||[],points=series?.points||[];
  const ids=keys.map(key=>key.id),namespace=state.group,intentRevision=chartIntentRevision;
  const hidden=hiddenBarSeries(id,namespace,ids);
  // Keys represent actual records; a zero metric must not hide their original legends.
  const used=keys.map((key,index)=>({key,index}));
  const datasets=used.map(({key,index},i)=>{
    const color=chartColor(key,i);
    return {label:displayKey(key),data:points.map(p=>p.values[index]?.known_value==null?null:Number(p.values[index].known_value)),exactValues:points.map(p=>p.values[index]||unknownMetric()),
      backgroundColor:presentation.selectedIndex==null?color+'cc':points.map((_,pi)=>color+(pi===presentation.selectedIndex?'ee':'66')),
      borderColor:color,borderWidth:1,borderRadius:2};
  });
  const options=stackOpts(t(METRICS[state.metric].labelKey));
  options.plugins.legend.onClick=(event,item,legend)=>{
    const chart=legend.chart;
    if(chart!==charts[id]||chart.$intentRevision!==intentRevision||intentRevision!==chartIntentRevision)return;
    Chart.defaults.plugins.legend.onClick(event,item,legend);
    rememberBarVisibility(id,chart);
  };
  options.plugins.tooltip.filter=it=>{const v=it.dataset.exactValues[it.dataIndex];return v.state!=='exact'||BigInt(v.known_value||'0')!==0n;};
  options.plugins.tooltip.callbacks.label=it=>it.dataset.label+': '+tooltipMetricText(it.dataset.exactValues[it.dataIndex]);
  options.plugins.tooltip.callbacks.title=items=>{
    if(!items.length)return '';
    const total=sumMetricValues(items.map(it=>it.dataset.exactValues[it.dataIndex]));
    return items[0].label+' · '+t('sub.total',tooltipMetricText(total));
  };
  if(onClick)options.onClick=(event,elements)=>{if(intentRevision===chartIntentRevision&&elements?.length)onClick(points[elements[0].index],elements[0].index);};
  mkChart(id,{type:'bar',$seriesIds:ids,$seriesNamespace:namespace,$intentRevision:intentRevision,$hiddenSeriesIds:hidden,
    data:{labels:points.map(p=>presentation.label?presentation.label(p):p.label),datasets},options});
}
function daySeries(day) {
  const hourlyKeys=day?.keys||[],daily=day?.day_only_series,hasUnallocated=!!daily?.keys?.length;
  const keys=[...new Map([...hourlyKeys,...(daily?.keys||[])].map(key=>[key.id,key])).values()];
  const hourlyIndex=new Map(hourlyKeys.map((key,index)=>[key.id,index]));
  const hours=hasUnallocated&&!hourlyKeys.length?[]:(day?.hours||Array.from({length:24},(_,hour)=>({hour,values:[]})));
  const points=hours.map(hour=>({label:String(hour.hour).padStart(2,'0')+(hour.state==='nonexistent'?' ∅':hour.intervals?.length>1?' ↺':''),
    values:keys.map(key=>hour.values?.[hourlyIndex.get(key.id)]||unknownMetric())}));
  if(hasUnallocated){
    const dailyIndex=new Map(daily.keys.map((key,index)=>[key.id,index]));
    points.push({label:t('bucket.unallocatedTime'),values:keys.map(key=>daily.values?.[dailyIndex.get(key.id)]||unknownMetric())});
  }
  return {keys,points,hasUnallocated};
}
// Rendering may temporarily fall back, while the user's current-page choice remains 3D.
let dailyRenderedView=state.view,daily3dRevision=0;
function setDailyViewUi() {
  $('#daily-view').setAttribute('aria-label',t('view.label'));
  document.querySelectorAll('#daily-view button').forEach(button=>{
    const active=button.dataset.v===dailyRenderedView;
    button.classList.toggle('on',active);button.setAttribute('aria-pressed',String(active));
    button.title=state.view==='3d'&&dailyRenderedView!=='3d'?t('view.fallback'):'';
  });
  $('#daily3d-tools').hidden=dailyRenderedView!=='3d';
  $('#daily3d-auto').textContent=state.autoRotate?'⏸':'▶';
  $('#daily3d-auto').title=t(state.autoRotate?'view.stop':'view.auto');
}
function changeDailyView(view) {
  if(view!=='3d'&&view!=='standard')return;
  state.view=view;
  // Preserve the already displayed chart until its query matches the controls.
  // The chosen view will be applied when that query succeeds.
  if(v2View&&v2View.key===currentQueryKey())renderCharts();else if(!v2View){dailyRenderedView=view;setDailyViewUi();}
}
function destroyDailyChart() {
  const chart=charts['#chartDaily'];if(chart){try{chart.destroy();}catch{}delete charts['#chartDaily'];}
}
function drawMainSeries(series,onClick) {
  const renderRevision=++daily3dRevision;
  if(state.view==='3d'){
    const keys=series?.keys||[],points=series?.points||[],ids=keys.map(key=>key.id),namespace=state.group,intentRevision=chartIntentRevision;
    const current=()=>renderRevision===daily3dRevision&&intentRevision===chartIntentRevision&&namespace===state.group&&state.view==='3d';
    const hidden=hiddenBarSeries('#chartDaily',namespace,ids);
    const used=keys.map((key,index)=>({key,index})).filter(item=>!hidden.has(item.key.id));
    const ok=window.Usagebook3D?.update({
      container:$('#daily3d-stage'),legend:$('#daily3d-legend'),
      keys:used.map(({key,index})=>({id:key.id,label:displayKey(key),color:chartColor(key,index)})),
      legendKeys:keys.map((key,index)=>({id:key.id,label:displayKey(key),color:chartColor(key,index),hidden:hidden.has(key.id)})),
      legendToggleTitle:key=>t(key.hidden?'view.showSeries':'view.hideSeries',key.label),
      onToggleSeries:id=>{
        if(!current()||!ids.includes(id))return;
        const entry=barVisibility['#chartDaily'],key=barSeriesKey(namespace,id);
        if(entry.hidden.has(key))entry.hidden.delete(key);else entry.hidden.set(key,entry.seen);
        drawMainSeries(series,onClick);
      },
      points:points.map(point=>({...point,values:used.map(item=>point.values[item.index]||unknownMetric())})),
      formatValue:metricText,autoRotate:state.autoRotate,
      onClick:onClick?((point,index)=>{if(current())onClick(points[index],index);}):null,
    });
    if(ok){
      destroyDailyChart();$('#chartDaily').hidden=true;$('#daily3d').hidden=false;
      const fallback=document.getElementById('chartDaily-fallback');if(fallback)fallback.hidden=true;
      dailyRenderedView='3d';
      window.Usagebook3D.pause(false);setDailyViewUi();return;
    }
  }
  dailyRenderedView='standard';
  window.Usagebook3D?.pause(true);$('#daily3d').hidden=true;$('#chartDaily').hidden=false;
  setDailyViewUi();drawSeries('#chartDaily',series,onClick);
}
function renderCharts() {
  const d=v2View.response.payload.data,single=isDayWin(state.win);
  const day=daySeries(v2View.day),dayTitle=day.hasUnallocated?t('chart.unallocatedTime'):'';
  const reportTimezone=v2View.response.stamp.report_timezone;
  $('#day-prev').hidden=!single;$('#day-next').hidden=!single;$('#day-back').hidden=!single||state.win==='today';
  $('#hourly-panel').hidden=single;$('#grid2').classList.toggle('single',single);
  $('#daily-h2').textContent=t(single?(day.hasUnallocated?'h2.dayUsage':'h2.dayHourly'):'h2.daily');
  $('#daily-h2').title=single?dayTitle:'';
  $('#hourly-h2').title='';
  const metric=d.totals.metrics[METRIC_IDS[state.metric]];
  $('#daily-sub').textContent=single&&metric.state==='exact'&&metric.known_value==='0'?t('empty.noSel'):t(single?'sub.dayTotal':'sub.total',metricText(metric));
  drawMainSeries(single?day:d.series,single?null:point=>{
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
      $('#hourly-h2').textContent=t(day.hasUnallocated?'h2.dayUsage':'h2.hourly');$('#hourly-h2').title=dayTitle;
      $('#hourly-sub').textContent=v2View.day?t('sub.hourlyDay',v2View.day.date,metricText(v2View.day.total.metrics[METRIC_IDS[state.metric]])):t('empty.none');
      drawSeries('#chartHourly',day);
    }
  }
  renderDoughnut();
}
function dateInReportZone(ms,reportTimezone) {
  const parts=calendarDayFormatter(reportTimezone).formatToParts(new Date(Number(ms)));
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
  mkChart('#chartSource',{type:'doughnut',$entityIds:ids,$intentRevision:chartIntentRevision,$hiddenEntityIds:new Set(doughnutHiddenIds),data:{labels:rows.map(r=>`${displayKey(r)} ${ratioText(r.value,{state:d.complete?'exact':'partial',known_value:total.toString()})}`),datasets},options,plugins:[centerTextPlugin],$center:{v:formatInteger(total),l:t('dough.center',t(METRICS[state.metric].labelKey))}});
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
  const page=v2View.tables[dim],elId='tbl-'+dim;
  $('#more-'+dim).hidden=!page.next_cursor;
  if(tblMemoKey()===_tblKeys[elId])return;
  const model=dim==='model',cw=cwVisible(),sel=model?state.mdlSel:state.srcSel;
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
  setTableHTML(elId,html);
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
  const view=v2View,signal=v2Controller?.signal;if(!view||view.key!==currentQueryKey()||view.tableBusy.has(dim)||!view.tables[dim].next_cursor)return;
  const block=tableLimitBlocks.get(dim);
  if(block?.blocked&&block.key===view.key&&block.stamp===stampKey(view.response.stamp))return;
  view.tableBusy.add(dim);renderDetailTable(dim);
  try{const result=await apiV2('query',{kind:'table',query:{...view.query,metric:'total'},dimension:dim,scope:'facet',page:{limit:100,cursor:view.tables[dim].next_cursor}},signal);
    if(view!==v2View||view.key!==currentQueryKey())return;verifyStamp(view.response,result);const page=result.payload.data;
    view.tables[dim]={...page,rows:[...view.tables[dim].rows,...page.rows]};tableLimitBlocks.delete(dim);clearSelectionIssue('table',dim);registerLabels(dim,page.rows);DATA.view_generation=++viewGeneration;
  }catch(e){if(e.name!=='AbortError'&&!signal?.aborted&&view===v2View&&view.key===currentQueryKey()){if(['CURSOR_STALE','TIMEZONE_CHANGED'].includes(e.code))load(true);else if(['QUERY_LIMIT','BODY_TOO_LARGE'].includes(e.code)){tableLimitBlocks.set(dim,{key:view.key,stamp:stampKey(view.response.stamp),cursor:view.tables[dim].next_cursor,blocked:true});showSelectionIssue('table',dim);}else showLoadFailure(e);}}
  finally{view.tableBusy.delete(dim);if(view===v2View&&view.key===currentQueryKey())renderDetailTable(dim);}
}
function renderSelectorButtons() {
  for(const dim of ['source','model']){
    const sel=dim==='source'?state.srcSel:state.mdlSel,button=$(dim==='source'?'#src-ms-btn':'#mdl-ms-btn'),ids=catalogItems(dim).map(x=>x.id);
    let label=t(dim==='source'?'ms.allSrc':'ms.allMdl'),sub=ids.length?t('ms.nItems',ids.length):'';
    if(sel){const live=ids.filter(id=>selected(sel,id));
      if(sel.exclude&&!ids.length){label=words('已筛选','Filtered');sub='';}
      else if(!live.length){label=t(dim==='source'?'ms.noSrc':'ms.noMdl');sub='';}
      else if(live.length===1){label=labels[dim].get(live[0])||live[0];sub=t('ms.oneItem');}
      else {label=t('ms.selected',live.length,ids.length);sub='';}
    }
    button.classList.toggle('filter-on',!!sel);button.innerHTML=`<span class="lbl">${escapeHtml(label)}</span> <span class="cnt">${sub?escapeHtml(sub)+' · ':''}▾</span>`;
  }
}
function filterRankingQuery(view) {
  return {...view.query,as_of_ms:view.response.as_of_ms,sources:{mode:'all'},models:{mode:'all'},metric:'total'};
}
function filterWindowKey(){return v2View?JSON.stringify([v2View.key,filterRankingQuery(v2View),v2View.response.stamp,v2View.response.as_of_ms]):'';}
function syncFilterCatalog(dim) {
  const panel=filterPanels[dim];if(!panel||panel.element.hidden)return;
  if(panel.controller.signal.aborted||panel.key!==filterWindowKey()){buildFilterPanel(dim);return;}
  // The directory only supplies identities/labels; it does not invalidate the
  // current-window ranking already being read from the displayed snapshot.
  panel.items=catalogItems(dim);
  panel.element.querySelector('.ms-search').placeholder=t('ms.search',catalogs[dim].total);
  renderFilterList(dim);
}
function cancelFilterRead(dim) {
  const panel=filterPanels[dim];if(!panel)return;
  panel.controller.abort();panel.sequence++;panel.busy=false;
}
function buildFilterPanel(dim) {
  if(!v2View)return;
  const element=$(dim==='source'?'#src-ms-panel':'#mdl-ms-panel'),previous=filterPanels[dim];
  const scroll=element.querySelector('.ms-list')?.scrollTop||0;
  previous?.controller.abort();
  const panel=filterPanels[dim]={element,key:filterWindowKey(),view:v2View,query:filterRankingQuery(v2View),search:element.dataset.q||'',items:catalogItems(dim),next:null,stats:new Map(),statsNext:null,statsDone:false,busy:false,sequence:0,visibleLimit:previous?.visibleLimit||200,controller:new AbortController()};
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
  (async()=>{await readFilterPage(dim,false);while(panel===filterPanels[dim]&&!panel.controller.signal.aborted&&panel.key===filterWindowKey()&&panel.view.key===currentQueryKey()&&panel.statsNext)await readFilterPage(dim,true);})();
}
async function readFilterPage(dim,append) {
  const panel=filterPanels[dim];if(!panel||panel.busy)return;
  const view=panel.view;
  if(panel.key!==filterWindowKey()||view.key!==currentQueryKey()||panel.controller.signal.aborted)return;
  const seq=++panel.sequence;panel.busy=true;
  try{
    const query=panel.query;
    const ranking=await apiV2('query',{kind:'table',query,dimension:dim,scope:'facet',page:{limit:200,cursor:append?panel.statsNext:null}},panel.controller.signal);
    if(panel!==filterPanels[dim]||seq!==panel.sequence||panel.key!==filterWindowKey()||view.key!==currentQueryKey())return;
    verifyStamp(view.response,ranking);
    const data=ranking.payload.data;if(!append)panel.stats.clear();
    for(const row of data.rows)panel.stats.set(row.id,row);
    panel.statsNext=data.next_cursor;panel.statsDone=!data.next_cursor;registerLabels(dim,data.rows);
    panel.element.querySelector('.ms-error').textContent='';clearSelectionIssue('panel',dim);
  }catch(e){if(panel===filterPanels[dim]&&seq===panel.sequence&&panel.key===filterWindowKey()&&view.key===currentQueryKey()&&e.name!=='AbortError'){
    panel.statsNext=null;
    if(['CURSOR_STALE','TIMEZONE_CHANGED'].includes(e.code)){panel.key='';load(true);}else if(['QUERY_LIMIT','BODY_TOO_LARGE'].includes(e.code)){showSelectionIssue('panel',dim);}else{
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
function refreshSavedView(done) {
  // A user-requested read permits one retry of a previously limited page.
  for(const block of tableLimitBlocks.values())block.blocked=false;
  if(manualRefresh)manualRefresh.finish(false,false,true);
  const op={finished:false,controller:new AbortController(),timer:null};manualRefresh=op;
  const active=()=>manualRefresh===op&&!op.finished;
  op.finish=(timedOut=false,failed=false,superseded=false,selectionLimited=false,queryLimited=false)=>{
    if(!active())return;
    op.finished=true;clearTimeout(op.timer);op.controller.abort();
    manualRefresh=null;refreshBusy=false;if(!failed)refreshTimedOut=timedOut;
    $('#btn-refresh').disabled=false;$('#btn-refresh').textContent=t('btn.refresh');
    if((timedOut||failed)&&!selectionLimited&&!queryLimited&&!recoveryUntil){recoveryUntil=Date.now()+RETRY_WAIT_MS;recoveryBase=null;}
    // The normal load path records source/storage errors separately. A manual
    // read completes as soon as its saved result arrives, even during collection.
    if(!selectionLimited&&!queryLimited&&!selectionIssues.has('query:model')&&(!superseded||pendingQueryKey===null))retryInBackground();renderStatus();
    if(done)done({ok:!timedOut&&!failed&&!superseded,timedOut,failed,...(superseded?{superseded:true}:{}),...(selectionLimited?{selectionLimited:true}:{}),...(queryLimited?{queryLimited:true}:{})});
  };
  refreshBusy=true;$('#btn-refresh').disabled=true;$('#btn-refresh').textContent=t('btn.refreshing');renderStatus();
  op.timer=setTimeout(()=>op.finish(true),RETRY_WAIT_MS);
  // The operation only owns this read's abort signal. Its deadline cannot
  // cancel a later filter change or an independent automatic read.
  void load(true,op.controller.signal).then(result=>{
    if(active())op.finish(false,!result?.ok&&!result?.superseded,!!result?.superseded,!!result?.selectionLimited,!!result?.queryLimited);
  },error=>{
    if(active()){showLoadFailure(error);op.finish(false,true);}
  });
}

/* ---------------- 启动 ---------------- */
// 旧布局脚本已完成控件绑定与偏好恢复，这里是唯一的数据启动点。
migrateModels();
window.addEventListener('scroll',loadVisibleTablePages,{passive:true});
window.addEventListener('resize',loadVisibleTablePages);
document.querySelectorAll('#daily-view button').forEach(button=>button.addEventListener('click',()=>changeDailyView(button.dataset.v)));
$('#daily3d-reset').addEventListener('click',()=>window.Usagebook3D?.reset());
$('#daily3d-top').addEventListener('click',()=>window.Usagebook3D?.top());
$('#daily3d-left').addEventListener('click',()=>window.Usagebook3D?.rotate(-1));
$('#daily3d-right').addEventListener('click',()=>window.Usagebook3D?.rotate(1));
$('#daily3d-zoom-in').addEventListener('click',()=>window.Usagebook3D?.zoom(1));
$('#daily3d-zoom-out').addEventListener('click',()=>window.Usagebook3D?.zoom(-1));
$('#daily3d-auto').addEventListener('click',()=>{state.autoRotate=!state.autoRotate;window.Usagebook3D?.setAutoRotate(state.autoRotate);setDailyViewUi();});
setDailyViewUi();
applyArf();
setInterval(()=>{if(!DATA?.ready||document.hidden)return;if(renderedDay!==localDayStr())render();else renderStatus();},1000);
load();

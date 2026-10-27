import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { RoundedBoxGeometry } from 'three/addons/geometries/RoundedBoxGeometry.js';

/* Native dashboard renderer. Query state, decimal formatting and drill-down stay in dashboard_v2.js. */
const MAX_INSTANCES = 5000;
let context = null;

function textSprite(text, color = '#8b9bad') {
  const canvas = document.createElement('canvas');
  canvas.width = 256; canvas.height = 64;
  const ctx = canvas.getContext('2d');
  ctx.font = '500 25px -apple-system, PingFang SC, sans-serif';
  ctx.textAlign = 'center'; ctx.textBaseline = 'middle'; ctx.fillStyle = color;
  ctx.fillText(String(text), 128, 32);
  const texture = new THREE.CanvasTexture(canvas); texture.colorSpace = THREE.SRGBColorSpace;
  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: texture, transparent: true, depthTest: false }));
  sprite.scale.set(1.9, .48, 1); sprite.renderOrder = 10;
  return sprite;
}

function supported() {
  try { return !!document.createElement('canvas').getContext('webgl2'); } catch { return false; }
}

// Keep the fitted view separate from camera.zoom so a refresh never undoes a user's zoom.
function fitCamera(c) {
  const width = c.container.clientWidth, height = c.container.clientHeight;
  if (!width || !height || !c.bounds) return;
  const { camera, controls, bounds } = c;
  const center = bounds.getCenter(new THREE.Vector3()), size = bounds.getSize(new THREE.Vector3());
  const radius = size.length() / 2 + center.distanceTo(controls.target) + 2;
  const offset = camera.position.clone().sub(controls.target);
  camera.position.copy(controls.target).add(offset.setLength(Math.max(18, radius)));
  camera.lookAt(controls.target); camera.updateMatrixWorld();
  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity, minZ = Infinity, maxZ = -Infinity;
  const view = camera.matrixWorldInverse.elements;
  const include = (x, y, z, halfX, halfY, halfZ = 0) => {
    minX = Math.min(minX, x - halfX); maxX = Math.max(maxX, x + halfX);
    minY = Math.min(minY, y - halfY); maxY = Math.max(maxY, y + halfY);
    minZ = Math.min(minZ, z - halfZ); maxZ = Math.max(maxZ, z + halfZ);
  };
  // Project each real box using its center and absolute transformed half axes.
  // A scene-wide box invents tall corners over empty cells; it makes sparse data unnecessarily small.
  const boxes = c.fitBoxes;
  for (let i = 0; i < boxes.length; i += 6) {
    const x = boxes[i], y = boxes[i + 1], z = boxes[i + 2], hx = boxes[i + 3], hy = boxes[i + 4], hz = boxes[i + 5];
    include(
      view[0] * x + view[4] * y + view[8] * z + view[12],
      view[1] * x + view[5] * y + view[9] * z + view[13],
      view[2] * x + view[6] * y + view[10] * z + view[14],
      Math.abs(view[0]) * hx + Math.abs(view[4]) * hy + Math.abs(view[8]) * hz,
      Math.abs(view[1]) * hx + Math.abs(view[5]) * hy + Math.abs(view[9]) * hz,
      Math.abs(view[2]) * hx + Math.abs(view[6]) * hy + Math.abs(view[10]) * hz,
    );
  }
  // Sprites face the camera: project their centers, then include their screen-plane extents.
  for (const label of c.labelGroup.children) {
    const { x, y, z } = label.position;
    include(view[0] * x + view[4] * y + view[8] * z + view[12], view[1] * x + view[5] * y + view[9] * z + view[13], view[2] * x + view[6] * y + view[10] * z + view[14], label.scale.x / 2, label.scale.y / 2);
  }
  const aspect = width / height, padding = Math.min(20, width * .06, height * .06);
  const viewHeight = Math.max(1, (maxY - minY) / (1 - padding * 2 / height), (maxX - minX) / aspect / (1 - padding * 2 / width));
  const centerX = (minX + maxX) / 2, centerY = (minY + maxY) / 2;
  camera.left = centerX - viewHeight * aspect / 2; camera.right = centerX + viewHeight * aspect / 2;
  camera.top = centerY + viewHeight / 2; camera.bottom = centerY - viewHeight / 2;
  camera.near = Math.max(.1, -maxZ - 2); camera.far = Math.max(camera.near + 1, -minZ + 2);
  camera.updateProjectionMatrix();
}

function updateLegend(c) {
  const { payload } = c, legend = payload.legend;
  if (!legend) return;
  const focusedId = legend.contains(document.activeElement) ? document.activeElement.dataset.seriesId : null;
  const existing = new Map(Array.from(legend.children, item => [item.dataset.seriesId, item]));
  const desired = [];
  for (const key of payload.legendKeys || payload.keys) {
    const id = String(key.id), item = existing.get(id) || document.createElement('button');
    if (!existing.has(id)) {
      item.type = 'button'; item.dataset.seriesId = id;
      item.append(document.createElement('i'), document.createElement('span'));
      item.addEventListener('click', () => { if (context?.payload?.legend === legend) context.payload.onToggleSeries?.(id); });
    }
    item.setAttribute('aria-pressed', String(!key.hidden));
    item.title = payload.legendToggleTitle?.(key) || key.label;
    item.firstElementChild.style.background = key.color;
    item.lastElementChild.textContent = key.label;
    desired.push(item); existing.delete(id);
  }
  for (const item of existing.values()) item.remove();
  desired.forEach((item, index) => { if (legend.children[index] !== item) legend.insertBefore(item, legend.children[index] || null); });
  if (focusedId != null && document.activeElement?.dataset.seriesId !== focusedId) desired.find(item => item.dataset.seriesId === focusedId)?.focus({ preventScroll: true });
}

function mount(container) {
  if (context?.container === container) return true;
  destroy();
  if (!container || !supported()) return false;
  let renderer;
  try { renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true, powerPreference: 'high-performance' }); }
  catch { return false; }
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.8));
  renderer.setClearColor(0x0b0e14, 0); renderer.outputColorSpace = THREE.SRGBColorSpace;
  renderer.domElement.setAttribute('aria-hidden', 'true');
  container.appendChild(renderer.domElement);

  const scene = new THREE.Scene();
  const camera = new THREE.OrthographicCamera(-8, 8, 4, -4, .1, 100);
  const defaultPosition = new THREE.Vector3(10, 8.5, 12); camera.position.copy(defaultPosition);
  const controls = new OrbitControls(camera, renderer.domElement);
  controls.target.set(0, 1.25, 0); controls.enableDamping = true; controls.dampingFactor = .075;
  controls.enablePan = false; controls.minPolarAngle = .18; controls.maxPolarAngle = 1.28;
  controls.minZoom = .65; controls.maxZoom = 2.7; controls.autoRotateSpeed = .65; controls.update();

  scene.add(new THREE.HemisphereLight(0xb9d9ff, 0x101721, 2.1));
  const key = new THREE.DirectionalLight(0xd7e9ff, 3.4); key.position.set(4, 10, 7); scene.add(key);
  const rim = new THREE.DirectionalLight(0x6cb6ff, 1.8); rim.position.set(-7, 4, -5); scene.add(rim);
  const platform = new THREE.Mesh(new THREE.BoxGeometry(1, .1, 1), new THREE.MeshStandardMaterial({ color: 0x111a26, roughness: .8, metalness: .15, transparent: true, opacity: .92 }));
  platform.position.y = -.08; scene.add(platform);

  const labelGroup = new THREE.Group(); scene.add(labelGroup);
  const geometry = new RoundedBoxGeometry(1, 1, 1, 2, .07);
  const material = new THREE.MeshStandardMaterial({ color: 0xffffff, roughness: .32, metalness: .12 });
  material.onBeforeCompile = shader => {
    shader.vertexShader = shader.vertexShader.replace('#include <common>', '#include <common>\nvarying float vBarHeight;').replace('#include <begin_vertex>', '#include <begin_vertex>\nvBarHeight = clamp(position.y + 0.5, 0.0, 1.0);');
    shader.fragmentShader = shader.fragmentShader.replace('#include <common>', '#include <common>\nvarying float vBarHeight;').replace('#include <color_fragment>', `#include <color_fragment>
      float barTop = smoothstep(0.05, 1.0, vBarHeight);
      diffuseColor.rgb *= mix(0.58, 1.24, barTop);
      diffuseColor.rgb = mix(diffuseColor.rgb, vec3(1.0), pow(vBarHeight, 5.0) * 0.2);
    `).replace('#include <emissivemap_fragment>', `#include <emissivemap_fragment>
      totalEmissiveRadiance += diffuseColor.rgb * pow(vBarHeight, 3.0) * 0.18;
    `);
  };
  const mesh = new THREE.InstancedMesh(geometry, material, MAX_INSTANCES); mesh.count = 0;
  mesh.instanceMatrix.setUsage(THREE.DynamicDrawUsage); mesh.frustumCulled = false; scene.add(mesh);

  const tooltip = document.createElement('div'); tooltip.className = 'daily3d-tooltip'; tooltip.hidden = true;
  container.appendChild(tooltip);
  const matrix = new THREE.Matrix4(), position = new THREE.Vector3(), scale = new THREE.Vector3(), quaternion = new THREE.Quaternion(), color = new THREE.Color();
  const raycaster = new THREE.Raycaster(), pointer = new THREE.Vector2();
  const c = context = {
    container, renderer, scene, camera, controls, defaultPosition, platform, labelGroup, mesh, tooltip,
    matrix, position, scale, quaternion, color, raycaster, pointer,
    payload: null, grid: null, baseColors: [], visibleInstances: [], hoverId: null, touchId: null, pointers: new Map(),
    paused: false, hidden: false, frame: 0, previous: 0, xStep: .5, zStep: .8,
    dirty: false, inFrame: false, settling: false, stableFrames: 0, bounds: null, fitBoxes: null, cleanup: [],
  };

  const visible = () => context === c && !c.paused && !c.hidden && !document.hidden;
  const animate = time => {
    c.frame = 0;
    if (!visible()) return;
    c.inFrame = true;
    const delta = c.previous ? Math.min((time - c.previous) / 1000, .05) : 1 / 60;
    c.previous = time;
    const changed = controls.update(delta);
    if (changed) { c.settling = true; c.stableFrames = 0; }
    else if (++c.stableFrames >= 3) c.settling = false;
    if (c.dirty || changed) { c.dirty = false; renderer.render(scene, camera); }
    c.inFrame = false;
    if (controls.autoRotate || c.settling || c.dirty) c.frame = requestAnimationFrame(animate);
    else c.previous = 0;
  };
  const requestRender = c.requestRender = () => {
    c.dirty = true;
    if (visible() && !c.frame && !c.inFrame) c.frame = requestAnimationFrame(animate);
  };
  const stop = c.stop = () => { cancelAnimationFrame(c.frame); c.frame = 0; c.previous = 0; };
  const listen = (target, name, handler, options) => {
    target.addEventListener(name, handler, options);
    c.cleanup.push(() => target.removeEventListener(name, handler, options));
  };

  const hitTest = event => {
    const rect = renderer.domElement.getBoundingClientRect();
    if (!rect.width || !rect.height || !mesh.count) return null;
    camera.updateMatrixWorld();
    pointer.set(((event.clientX - rect.left) / rect.width) * 2 - 1, -((event.clientY - rect.top) / rect.height) * 2 + 1);
    raycaster.setFromCamera(pointer, camera);
    return raycaster.intersectObject(mesh, false).find(hit => c.visibleInstances[hit.instanceId])?.instanceId ?? null;
  };
  const recolor = () => {
    if (context !== c) return;
    const { payload } = c;
    for (let id = 0; id < mesh.count; id++) {
      color.copy(c.baseColors[id]);
      if (c.hoverId === id) color.lerp(new THREE.Color(0xffffff), .48);
      mesh.setColorAt(id, color);
    }
    if (mesh.instanceColor) mesh.instanceColor.needsUpdate = true;
    if (payload) payload.onHover?.(c.hoverId);
    requestRender();
  };
  const setHover = id => { if (c.hoverId !== id) { c.hoverId = id; recolor(); } };
  const dismiss = c.dismiss = () => { tooltip.hidden = true; c.touchId = null; setHover(null); };
  const showTooltip = (id, event) => {
    if (id === null || !c.payload) { dismiss(); return; }
    const { keys, points, formatValue } = c.payload;
    const seriesIndex = id % keys.length, timeIndex = Math.floor(id / keys.length);
    const key = keys[seriesIndex], point = points[timeIndex], value = point.values[seriesIndex];
    tooltip.replaceChildren();
    const date = document.createElement('span'); date.textContent = point.label;
    const name = document.createElement('strong'); name.textContent = key.label;
    const dot = document.createElement('i'); dot.style.background = key.color; name.prepend(dot);
    const number = document.createElement('b'); number.textContent = formatValue(value);
    tooltip.append(date, name, number);
    tooltip.hidden = false;
    const viewport = window.visualViewport;
    const left = (viewport?.offsetLeft || 0) + 8, top = (viewport?.offsetTop || 0) + 8;
    const right = left + (viewport?.width || window.innerWidth) - 16, bottom = top + (viewport?.height || window.innerHeight) - 16;
    const rect = tooltip.getBoundingClientRect();
    const above = event.clientY - rect.height - 14;
    tooltip.style.left = `${Math.max(left, Math.min(event.clientX - rect.width / 2, right - rect.width))}px`;
    tooltip.style.top = `${Math.max(top, Math.min(above >= top ? above : event.clientY + 14, bottom - rect.height))}px`;
  };
  const onPointerMove = event => {
    const down = c.pointers.get(event.pointerId);
    if (down && Math.hypot(event.clientX - down.x, event.clientY - down.y) > 6) { down.moved = true; dismiss(); }
    if (context !== c || event.pointerType === 'touch' || event.buttons || down) return;
    const id = hitTest(event); setHover(id); showTooltip(id, event);
    renderer.domElement.style.cursor = id !== null && c.payload?.onClick ? 'pointer' : 'grab';
  };
  const onPointerDown = event => {
    c.pointers.set(event.pointerId, { x: event.clientX, y: event.clientY, moved: event.button !== 0 });
    if (c.pointers.size > 1) { c.pointers.forEach(pointer => { pointer.moved = true; }); dismiss(); }
  };
  const onPointerUp = event => {
    const down = c.pointers.get(event.pointerId); c.pointers.delete(event.pointerId);
    if (context !== c || !down || down.moved || Math.hypot(event.clientX - down.x, event.clientY - down.y) > 6) return;
    const id = hitTest(event), payload = c.payload;
    if (id === null || !payload) { dismiss(); return; }
    if (event.pointerType === 'touch' || event.pointerType === 'pen') {
      const selected = c.touchId === id && !tooltip.hidden;
      c.touchId = id; setHover(id); showTooltip(id, event);
      if (!selected) return;
    }
    if (!payload.onClick) return;
    const seriesIndex = id % payload.keys.length, timeIndex = Math.floor(id / payload.keys.length);
    dismiss();
    payload.onClick(payload.points[timeIndex], timeIndex, payload.keys[seriesIndex]);
  };
  const onPointerCancel = event => { c.pointers.delete(event.pointerId); dismiss(); };
  listen(renderer.domElement, 'pointermove', onPointerMove);
  listen(renderer.domElement, 'pointerdown', onPointerDown);
  listen(renderer.domElement, 'pointerup', onPointerUp);
  listen(renderer.domElement, 'pointercancel', onPointerCancel);
  listen(renderer.domElement, 'lostpointercapture', event => { c.pointers.delete(event.pointerId); });
  listen(renderer.domElement, 'pointerleave', event => { if (event.pointerType !== 'touch') dismiss(); });
  listen(window, 'scroll', dismiss, { capture: true, passive: true });
  listen(window, 'blur', () => { c.pointers.clear(); dismiss(); });
  listen(controls, 'change', () => {
    c.pointers.forEach(pointer => { pointer.moved = true; });
    dismiss(); fitCamera(c); c.settling = true; c.stableFrames = 0; requestRender();
  });
  listen(controls, 'start', requestRender);

  const resize = () => {
    if (context !== c) return;
    const width = container.clientWidth, height = container.clientHeight;
    if (!width || !height) return;
    dismiss(); fitCamera(c);
    renderer.setSize(width, height, false);
    requestRender();
  };
  c.resize = resize;
  c.observer = new ResizeObserver(resize); c.observer.observe(container);
  if (typeof IntersectionObserver !== 'undefined') {
    c.intersectionObserver = new IntersectionObserver(entries => {
      c.hidden = !entries[entries.length - 1].isIntersecting;
      if (visible()) resize(); else { stop(); dismiss(); }
    });
    c.intersectionObserver.observe(container);
  }
  listen(document, 'visibilitychange', () => { if (visible()) resize(); else { stop(); dismiss(); } });
  resize();
  return true;
}

function update(payload) {
  const count = (payload.keys?.length || 0) * (payload.points?.length || 0);
  if (!payload.points?.length || !(payload.legendKeys || payload.keys)?.length || count > MAX_INSTANCES || !payload.keys?.every(key => key.color)) return false;
  if (!mount(payload.container)) return false;
  const c = context;
  c.dismiss(); c.pointers.clear(); c.payload = payload;
  const { keys, points } = payload;
  c.xStep = points.length > 45 ? .34 : points.length > 24 ? .43 : .58;
  c.zStep = keys.length > 5 ? .66 : .82;
  const xWidth = Math.max(1, (points.length - 1) * c.xStep), zWidth = Math.max(1, (keys.length - 1) * c.zStep);
  const positiveValue = metric => {
    const raw = metric?.known_value == null || metric.state === 'unknown' ? 0 : Number(metric.known_value);
    return Number.isFinite(raw) && raw > 0 ? raw : 0;
  };
  const maxValue = Math.max(1, ...points.flatMap(point => point.values.map(positiveValue)));
  c.baseColors = []; c.visibleInstances = new Uint8Array(count); c.fitBoxes = new Float64Array((count + 2) * 6); let maxHeight = 0;
  for (let timeIndex = 0; timeIndex < points.length; timeIndex++) for (let seriesIndex = 0; seriesIndex < keys.length; seriesIndex++) {
    const id = timeIndex * keys.length + seriesIndex, metric = points[timeIndex].values[seriesIndex];
    const raw = positiveValue(metric), height = raw > 0 ? Math.max(.025, raw / maxValue * 4.4) : 0;
    maxHeight = Math.max(maxHeight, height);
    c.position.set(timeIndex * c.xStep - xWidth / 2, height / 2, seriesIndex * c.zStep - zWidth / 2);
    // Keep row-major instance IDs for tooltips/navigation while empty cells have
    // no visible geometry and cannot be selected by the raycaster.
    c.visibleInstances[id] = Number(raw > 0);
    c.scale.set(raw > 0 ? c.xStep * .62 : 0, height, raw > 0 ? c.zStep * .57 : 0); c.quaternion.identity();
    c.matrix.compose(c.position, c.quaternion, c.scale); c.mesh.setMatrixAt(id, c.matrix);
    if (raw > 0) c.fitBoxes.set([c.position.x, c.position.y, c.position.z, c.scale.x / 2, c.scale.y / 2, c.scale.z / 2], id * 6);
    c.baseColors[id] = new THREE.Color(keys[seriesIndex].color);
  }
  c.mesh.count = count; c.mesh.instanceMatrix.needsUpdate = true; c.mesh.computeBoundingBox(); c.mesh.computeBoundingSphere();
  for (let id = 0; id < count; id++) c.mesh.setColorAt(id, c.baseColors[id]);
  if (c.mesh.instanceColor) c.mesh.instanceColor.needsUpdate = true;
  c.platform.scale.set(xWidth + 1.35, 1, zWidth + 1.65);
  c.fitBoxes.set([0, -.08, 0, (xWidth + 1.35) / 2, .05, (zWidth + 1.65) / 2], count * 6);
  c.fitBoxes.set([0, .005, 0, xWidth / 2 + .55, 0, zWidth / 2 + .65], (count + 1) * 6);
  c.bounds = new THREE.Box3(new THREE.Vector3(-xWidth / 2 - .675, -.13, -zWidth / 2 - .825), new THREE.Vector3(xWidth / 2 + .675, maxHeight, zWidth / 2 + .825));

  if (c.grid) {
    c.scene.remove(c.grid); c.grid.geometry.dispose();
    (Array.isArray(c.grid.material) ? c.grid.material : [c.grid.material]).forEach(item => item.dispose());
  }
  const lines = [];
  for (let i = 0; i < points.length; i++) { const x = i * c.xStep - xWidth / 2; lines.push(x, 0, -zWidth / 2 - .65, x, 0, zWidth / 2 + .65); }
  for (let i = 0; i < keys.length; i++) { const z = i * c.zStep - zWidth / 2; lines.push(-xWidth / 2 - .55, 0, z, xWidth / 2 + .55, 0, z); }
  c.grid = new THREE.LineSegments(new THREE.BufferGeometry().setAttribute('position', new THREE.Float32BufferAttribute(lines, 3)), new THREE.LineBasicMaterial({ color: 0x29425a, transparent: true, opacity: .34 }));
  c.grid.position.y = .005; c.scene.add(c.grid);
  c.labelGroup.children.forEach(child => { if (child.isSprite) { child.material.map?.dispose(); child.material.dispose(); } }); c.labelGroup.clear();
  const dateStep = Math.max(1, Math.ceil(points.length / 8));
  points.forEach((point, index) => {
    if (index % dateStep && index !== points.length - 1) return;
    const label = textSprite(point.label); label.position.set(index * c.xStep - xWidth / 2, -.02, zWidth / 2 + 1.05); c.labelGroup.add(label);
  });
  keys.forEach((key, index) => {
    const label = textSprite(key.label, key.color); label.position.set(-xWidth / 2 - 1.25, .04, index * c.zStep - zWidth / 2); label.scale.set(1.55, .4, 1); c.labelGroup.add(label);
  });
  updateLegend(c);
  c.controls.autoRotate = !!payload.autoRotate; c.paused = false; c.resize();
  return true;
}

function changeView(change) {
  if (!context) return;
  const c = context, autoRotate = c.controls.autoRotate;
  c.dismiss();
  // Consume residual damping before assigning an exact button-selected view.
  c.controls.autoRotate = false; c.controls.enableDamping = false; c.controls.update();
  change(c); c.controls.update();
  c.controls.enableDamping = true; c.controls.autoRotate = autoRotate;
  fitCamera(c); c.requestRender();
}
function reset() { changeView(c => { c.camera.position.copy(c.defaultPosition); c.camera.zoom = 1; c.controls.target.set(0, 1.25, 0); }); }
function top() { changeView(c => { c.camera.position.set(0, 18, .01); c.controls.target.set(0, 1.25, 0); }); }
function rotate(direction) {
  changeView(c => {
    const offset = c.camera.position.clone().sub(c.controls.target), spherical = new THREE.Spherical().setFromVector3(offset);
    spherical.theta += direction * Math.PI / 4; c.camera.position.copy(c.controls.target).add(new THREE.Vector3().setFromSpherical(spherical));
  });
}
function zoom(direction) {
  if (!context) return; const c = context;
  c.dismiss(); c.camera.zoom = THREE.MathUtils.clamp(c.camera.zoom * (direction > 0 ? 1.2 : 1 / 1.2), c.controls.minZoom, c.controls.maxZoom);
  c.camera.updateProjectionMatrix(); c.requestRender();
}
function setAutoRotate(value) {
  if (!context) return;
  context.controls.autoRotate = !!value; context.dismiss(); context.requestRender();
}
function pause(value) {
  if (!context) return;
  context.paused = !!value;
  if (value) { context.stop(); context.dismiss(); } else context.resize();
}
function destroy() {
  if (!context) return; const c = context; context = null;
  c.stop(); c.observer?.disconnect(); c.intersectionObserver?.disconnect();
  c.cleanup.forEach(cleanup => cleanup()); c.controls.dispose();
  c.scene.traverse(object => {
    if (object.isSprite) { object.material.map?.dispose(); object.material.dispose(); }
    else if (object.isMesh || object.isLine || object.isPoints) {
      object.geometry.dispose(); (Array.isArray(object.material) ? object.material : [object.material]).forEach(item => item.dispose());
    }
  });
  c.renderer.dispose(); c.renderer.domElement.remove(); c.tooltip.remove();
}

window.Usagebook3D = { supported, update, reset, top, rotate, zoom, setAutoRotate, pause, destroy };

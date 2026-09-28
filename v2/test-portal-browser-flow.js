#!/usr/bin/env node
// Executes the served portal/worklet with synthetic packet timing only.
const assert = require('node:assert/strict');
const childProcess = require('node:child_process');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(`${__dirname}/T28.V2.Server/Portal.cs`, 'utf8');
const portal = source.match(/<script>([\s\S]*?)<\/script>/)?.[1];
const fleetSource = fs.readFileSync(__dirname + '/T28.V2.Server/FleetPortal.cs', 'utf8');
const worklet = source.match(/public const string Worklet = """([\s\S]*?)""";/)?.[1];
assert.ok(portal && worklet, 'served portal and worklet must exist');
for (const expected of ['class=channel-grid', 'class=channel-card', '<details id=admin class=secondary>', '<summary>Channel controls</summary>', '<form id=admin-form class=admin-controls novalidate>', 'id=plan disabled', 'id=channel disabled', 'Channel plan', 'id=apply type=submit disabled>Apply channel', 'Receiver settings loading', 'id=control-error class=validation role=status aria-live=polite', 'class=volume-control', 'grid-template-columns:auto minmax\\(0,1fr\\)', 'align-items:center', 'min-block-size:2.75rem', 'summary::after', 'role=status aria-live=polite', 'focus-visible', 'viewport-fit=cover', 'id=squelch type=number min=-100 max=0 step=1 disabled']) assert.match(source, new RegExp(expected), 'compact channel widget keeps its accessible portal contract');
for (const expected of ['id=receiver-selector','id=receiver-connectivity','id=receiver-state','id=receiver-contact','id=receiver-hardware','id=receiver-revisions','id=receiver-save','id=receiver-apply','id=receiver-start','id=receiver-stop','Apply the pending configuration while stopped before starting RF.','configuration-pending','Stop ends RF receiving only','/v2/receivers/','/configuration','Listener role cannot','Waiting for authoritative receiver state']) assert.ok(fleetSource.includes(expected), 'fleet portal keeps its truthful receiver contract: '+expected);
for (const script of [portal, worklet]) assert.equal(childProcess.spawnSync(process.execPath, ['--check', '-'], {input: script, encoding: 'utf8'}).status, 0, 'served JavaScript syntax');

let Processor;
class WorkletPort { postMessage(value) { this.toMain?.(value); } }
class AudioWorkletProcessor { constructor() { this.port = new WorkletPort(); } }
vm.runInNewContext(worklet, {AudioWorkletProcessor, Float32Array, Int16Array, registerProcessor: (_, p) => Processor = p});
const elements = new Map();
class Element { constructor(id) { this.value = id === 'volume' ? '1' : id === 'ctcss' ? '1622' : '-17'; this.textContent = ''; this.innerHTML=''; this.dataset={}; this.children=[]; } replaceChildren(...values){this.children=values;} }
for (const id of ['play', 'mute', 'volume', 'squelch', 'ctcss', 'plan', 'channel', 'apply', 'control-error', 'admin-form', 'squelch-status', 'channel-status', 'channel-name', 'channel-state', 'status']) elements.set(id, new Element(id));
const workletEvents = [];
class Port { postMessage(v) { this.messages.push(v); this.toWorklet?.({data:v}); } constructor() { this.messages=[]; } }
class Node {
  static instances=[];
  constructor() { this.port=new Port(); this.processor=new Processor(); this.port.toWorklet=v=>this.processor.port.onmessage?.(v); this.processor.port.toMain=v=>{workletEvents.push(v); this.port.onmessage?.({data:v});}; Node.instances.push(this); }
  connect(x) { return x; }
  render() { const output = new Float32Array(128); this.processor.process([], [[output]]); return output; }
}
class Context { static instances=[]; constructor() { this.destination={}; this.audioWorklet={addModule:async()=>{}}; Context.instances.push(this); } createGain(){return this.gain={gain:{value:1},connect:x=>x};} resume(){return Promise.resolve();} suspend(){return Promise.resolve();} }
class Socket { static instances=[]; constructor(url, protocol) { this.url=url;this.protocol=protocol;this.readyState=0;this.sent=[];Socket.instances.push(this); } send(v){this.sent.push(JSON.parse(v));} close(code,reason){this.closed=code;this.readyState=3;this.onclose?.({code,reason});} open(){this.readyState=1;this.onopen?.();} }
const state={generation:1,thresholdTenths:-1000,ctcssTenths:1622,profileId:'lab-demo-hill'};
const channels=[{profileId:'lab-demo-hill',number:'371',displayName:'Demo Hill',planId:'network-control',planName:'Demo location 52',planGroup:'Other approved categories'},{profileId:'synthetic-b',number:'TST',displayName:'Synthetic B',planId:'synthetic-plan',planName:'Synthetic plan',planGroup:'Test cell'}];
const sessionBody=csrf=>({csrf,state,channels,channel:{status:'applied'}});
const context={document:{visibilityState:'visible',getElementById:id=>elements.get(id),createElement:id=>new Element(id),addEventListener:()=>{}},window:{addEventListener:()=>{}},location:{protocol:'https:',host:'tailnet.example:9443'},fetch:async(path, options)=>({ok:true,json:async()=>path==='/v2/session'?sessionBody('test'):Object.assign(JSON.parse(options.body),{generation:1})}),crypto:{randomUUID:()=> '10000000-0000-4000-8000-000000000001'},AudioContext:Context,AudioWorkletNode:Node,WebSocket:Socket,DataView,Number,JSON,BigInt,__T28_TEST_INSTRUMENTATION__:null}; context.globalThis=context;
vm.runInNewContext(portal, context);
const flush=()=>new Promise(r=>setImmediate(r));
const packet=(attempt,sequence)=>{const value=new ArrayBuffer(1960),v=new DataView(value);[84,50,56,80,2,1].forEach((x,i)=>v.setUint8(i,x));v.setUint16(6,40);v.setBigUint64(8,BigInt(attempt));v.setBigUint64(16,BigInt(sequence));v.setBigUint64(24,BigInt(sequence*960));v.setUint32(32,48000);v.setUint16(36,960);v.setUint8(38,1);v.setUint8(39,1);return value;};
const controls=(socket,type)=>socket.sent.filter(v=>v.type===type);
(async()=>{
  const pausedCalls=[];let pausedSessions=0;
  context.fetch=async(path,options)=>{if(path==='/v2/session'){pausedSessions++;return {ok:true,json:async()=>sessionBody('paused-test')}};const request=JSON.parse(options.body);pausedCalls.push(request);return {ok:true,json:async()=>Object.assign(request,{generation:1})};};
  const submit=()=>elements.get('admin-form').onsubmit({preventDefault(){}});
  await flush();assert.equal(elements.get('channel-name').textContent,'371 - Demo Hill','current approved channel replaces the generic heading');assert.equal(elements.get('channel-state').textContent,'Active');assert.equal(elements.get('plan').children.length,2,'selector contains only server-approved plans');assert.equal(elements.get('channel').children.length,1,'selected plan filters server-approved channels');elements.get('squelch').value='';submit();await flush();assert.equal(pausedCalls.length,0,'blank Apply sends no receiver request');assert.equal(elements.get('squelch-status').textContent,'-100 dB; 162.2 Hz effective','blank Apply leaves the receiver acknowledgement unchanged');
  elements.get('squelch').value='-100';submit();await flush();await flush();assert.equal(pausedCalls.length,0,'already-effective settings do not send a receiver request');assert.equal(Socket.instances.length,0,'paused Apply does not start audio transport');assert.equal(Context.instances.length,0,'paused Apply does not create audio playback');assert.equal(elements.get('status').textContent,'','paused Apply does not alter Play intent');assert.equal(elements.get('squelch-status').textContent,'-100 dB; 162.2 Hz effective','only receiver state updates the effective summary');
  elements.get('squelch').value='-17';submit();await flush();await flush();assert.equal(pausedCalls.length,1,'paused Apply can restore the deterministic fixture');assert.equal(elements.get('squelch-status').textContent,'-17 dB; 162.2 Hz effective','paused fixture restoration is acknowledged in place');
  elements.get('play').onclick(); await flush(); await flush();
  assert.equal(elements.get('status').dataset.state,'connecting','Play reports a compact Connecting state before transport opens');
  const socket=Socket.instances[0], node=Node.instances[0];
  assert.equal(Socket.instances.length, 1); assert.equal(Context.instances.length, 1); assert.equal(node.constructor.instances.length, 1, 'one worklet');
  socket.open();
  assert.equal(elements.get('status').dataset.state,'live','an open transport reports a compact Live state');
  assert.deepEqual(socket.sent.slice(0,2).map(v=>[v.type,v.attempt,v.packets]), [['flow',1,undefined],['credit',1,4]], 'only four startup credits prime the current socket attempt');
  let maxDepth=0, logicalMs=0, renderFrames=0;
  let sourceSequence=0;
  for(let index=1;index<=3004;index++) {
    sourceSequence += index % 5 === 0 ? 6 : 1; // bounded live-edge retirement creates repeated legitimate source gaps
    logicalMs += 12 + (sourceSequence % 17); // repeated observed 12-28 ms arrival envelope for over sixty simulated seconds
    socket.onmessage({data:packet(1,sourceSequence)}); maxDepth=Math.max(maxDepth,node.processor.q.length+(node.processor.x?1:0)); renderFrames+=(12+(sourceSequence%17))*48;
    while(renderFrames>=128){node.render();renderFrames-=128;}
  }
  assert.ok(logicalMs>=60000, 'jitter fixture covers at least sixty simulated seconds');
  assert.equal(workletEvents.filter(v=>v.type==='prime').length,1,'exactly one initial four-packet prime: '+JSON.stringify(workletEvents.filter(v=>v.type==='prime')));
  assert.equal(workletEvents.filter(v=>v.type==='underrun').length,0,'observed delivery jitter has no post-prime underrun');
  assert.ok(maxDepth<=5,'queued plus currently-consumed PCM stays within the five-packet live bound');
  const played=controls(socket,'consume').map(v=>v.sequence); assert.ok(played.at(-1)>3000,'gapped ordered output acknowledges beyond 3,000 frames'); assert.deepEqual(played,[...new Set(played)],'played acknowledgement remains exact and ordered'); assert.ok(played.some((v,i)=>i>0&&v-played[i-1]>1),'accelerated fixture forces live-edge source sequence gaps'); assert.ok(controls(socket,'consume').every(v=>v.packets===1),'every runtime acknowledgement atomically grants one replacement'); assert.equal(socket.closed,undefined,'valid gapped playback does not close the socket');
  assert.equal(controls(socket,'credit').length,1,'only the initial four-packet prime uses standalone credit');
  const duplicate=played.at(-1),beforeDuplicate=socket.sent.length;node.processor.port.toMain({type:'consume',attempt:1,sequence:duplicate});node.processor.port.toMain({type:'consume',attempt:1,sequence:duplicate});assert.equal(Socket.instances.length,1,'coalesced duplicate worklet controls retain one socket');assert.equal(socket.closed,undefined,'coalesced duplicate worklet controls do not close the socket');assert.equal(socket.sent.length,beforeDuplicate+2,'each duplicate is one atomic consume control without a separately-racing credit');
  assert.equal(context.closeCategory({code:1008,reason:'invalid-played'}),'invalid-played','policy close status uses the server sanitized category');
  const squelchCalls=[],squelchResponses=[];
  context.fetch=(path,options)=>{if(path==='/v2/session')return Promise.resolve({ok:true,json:async()=>sessionBody('test')});const request=JSON.parse(options.body);squelchCalls.push(request);return new Promise(resolve=>squelchResponses.push(()=>resolve({ok:true,json:async()=>Object.assign(request,{generation:1})})));};
  elements.get('squelch').value='';elements.get('squelch').oninput?.();elements.get('squelch').value='-';elements.get('squelch').oninput?.();elements.get('squelch').value='-100';elements.get('squelch').oninput?.();assert.equal(squelchCalls.length,0,'mobile editing drafts never send a receiver request');assert.equal(elements.get('squelch').value,'-100','mobile blank and minus intermediates do not restore the draft');submit();await flush();
  assert.equal(squelchCalls.length,1,'Apply sends exactly one receiver request');assert.equal(squelchCalls[0].thresholdTenths,-1000);assert.equal(squelchCalls[0].expectedGeneration,1,'Apply is bound to the acknowledged active generation');assert.equal(elements.get('apply').disabled,true,'Apply is disabled while its acknowledgement is in flight');
  squelchResponses.shift()();await flush();await flush();assert.equal(elements.get('squelch-status').textContent,'-100 dB; 162.2 Hz effective','only the acknowledged receiver setting updates the effective summary');assert.equal(elements.get('apply').disabled,false,'Apply re-enables after acknowledgement');
  const acknowledgedCalls=squelchCalls.length;elements.get('squelch').value='-100.5';submit();await flush();assert.equal(squelchCalls.length,acknowledgedCalls,'fractional draft is rejected before the receiver protocol');assert.equal(elements.get('squelch').value,'-100.5','invalid Apply preserves the editable draft');assert.match(elements.get('control-error').textContent,/whole power value/,'invalid Apply provides an accessible validation message');assert.equal(elements.get('squelch-status').textContent,'-100 dB; 162.2 Hz effective','invalid Apply leaves the acknowledged effective summary unchanged');
  elements.get('squelch').value='-17';submit();await flush();assert.equal(squelchCalls.length,2,'a subsequent Apply restores the deterministic fixture');squelchResponses.shift()();await flush();await flush();assert.equal(elements.get('squelch-status').textContent,'-17 dB; 162.2 Hz effective','fixture restoration is acknowledged in place');
  elements.get('volume').value='.35';elements.get('volume').oninput();elements.get('mute').onclick();assert.equal(Context.instances[0].gain.gain.value,0,'mute remains on the GainNode');elements.get('mute').onclick();assert.equal(Context.instances[0].gain.gain.value,.35,'volume resume remains in place');
  await elements.get('play').onclick(); assert.equal(controls(socket,'flow').at(-1).paused,true,'pause grants no credit'); const pausedCredits=controls(socket,'credit').length; socket.onmessage({data:packet(1,4000)}); assert.equal(controls(socket,'credit').length,pausedCredits,'paused portal does not consume media');
  await elements.get('play').onclick(); await flush(); assert.equal(Socket.instances.length,1,'resume retains one socket'); assert.equal(controls(socket,'credit').at(-1).packets,4,'resume clears stale state and re-primes four packets');
  const newAttempt=controls(socket,'flow').at(-1).attempt; node.processor.port.toMain({type:'consume',attempt:newAttempt-1,sequence:9}); assert.equal(controls(socket,'credit').length,pausedCredits+1,'stale worklet callbacks cannot credit replacement attempt');
  for(let sequence=4001;sequence<=4004;sequence++){socket.onmessage({data:packet(newAttempt,sequence)});for(let q=0;q<8;q++)node.render();}
  const beforeStarve=workletEvents.length; for(let q=0;q<24;q++)node.render(); for(let q=0;q<8;q++){const out=node.render();assert.ok([...out].every(x=>x===0),'starvation zero-fills truthfully');}
  assert.equal(workletEvents.slice(beforeStarve).filter(v=>v.type==='underrun').length,1,'starvation makes one underrun transition: '+JSON.stringify(workletEvents.slice(beforeStarve)));
  assert.equal(controls(socket,'flow').at(-1).attempt,newAttempt+1,'underrun retires the old attempt on the same socket');
  assert.deepEqual(controls(socket,'credit').at(-1),{type:'credit',attempt:newAttempt+1,packets:4},'recovery grants exactly four replacement startup credits');
  assert.equal(Socket.instances.length,1,'underrun recovery does not reconnect');
  const controlsAfterRecovery=socket.sent.length;node.processor.port.toMain({type:'consume',attempt:newAttempt,sequence:77});assert.equal(socket.sent.length,controlsAfterRecovery,'old attempt callback cannot corrupt same-socket recovery');
  // A late in-flight frame retains its server-issued attempt.  It cannot be
  // relabelled as the re-prime attempt or generate a consume/credit control.
  const oldAttempt=newAttempt,oldFrame=packet(oldAttempt,4999),beforeOld=socket.sent.length,beforeOldPackets=node.port.messages.filter(v=>v.type==='packet').length; socket.onmessage({data:oldFrame}); assert.equal(socket.sent.length,beforeOld,'late old-attempt PCM is silently retired without a control'); assert.equal(node.port.messages.filter(v=>v.type==='packet').length,beforeOldPackets,'late old-attempt PCM is silently retired before the worklet');
  const reprimeAttempt=controls(socket,'flow').at(-1).attempt; for(let sequence=5001;sequence<=5004;sequence++)socket.onmessage({data:packet(reprimeAttempt,sequence)}); assert.equal(node.port.messages.filter(v=>v.type==='packet').length,beforeOldPackets+4,'current-attempt PCM continues after a dropped old frame'); for(let q=0;q<8;q++)node.render(); assert.equal(elements.get('status').textContent,'Connected','old-attempt retirement does not expose the internal re-prime detail'); assert.equal(elements.get('status').dataset.state,'live','Connected retains the Live status-chip state');
  assert.equal(workletEvents.filter(v=>v.type==='prime').length,3,'initial, resume, and one starvation recovery each prime exactly four packets');
  socket.onmessage({data:new ArrayBuffer(1)});assert.equal(socket.closed,1008,'malformed media fails closed'); assert.equal(elements.get('status').dataset.state,'blocked','policy failures remain a sanitized Blocked state');
  console.log('v2 browser jitter/credit regression passed (synthetic only; not physically/audibly verified)');
})().catch(error=>{console.error(error.stack||error);process.exitCode=1;});

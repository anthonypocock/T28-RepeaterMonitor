#!/usr/bin/env node
// Deterministic lifecycle regression. Uses synthetic timers and packet-free sockets only.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(`${__dirname}/T28.V2.Server/Portal.cs`, 'utf8');
const portal = source.match(/<script>([\s\S]*?)<\/script>/)?.[1];
assert.ok(portal, 'served v2 portal script is present');
const elements = new Map();
class Element { constructor(id) { this.id=id; this.value=id==='volume'?'1':id==='ctcss'?'1622':'-17'; this.textContent=id==='play'?'Play':''; this.innerHTML=''; this.dataset={}; this.children=[]; } replaceChildren(...values){this.children=values;} }
for (const id of ['play','mute','volume','squelch','ctcss','plan','channel','apply','control-error','admin-form','squelch-status','channel-status','channel-name','channel-state','status']) elements.set(id,new Element(id));
let nextTimer=1; const timers=new Map(), documentListeners=new Map(), windowListeners=new Map(), actions=new Map();
class Context { constructor(){this.destination={};this.state='suspended';this.audioWorklet={addModule:async()=>{}};Context.instances.push(this)} static instances=[];createGain(){return {gain:{value:1},connect:x=>x}}resume(){this.state='running';this.onstatechange?.();return Promise.resolve()}suspend(){this.state='suspended';this.onstatechange?.();return Promise.resolve()} }
class Node { constructor(){this.port={postMessage:()=>{}}} connect(x){return x} }
class Socket { constructor(){this.readyState=0;this.sent=[];Socket.instances.push(this)} static instances=[];send(value){this.sent.push(JSON.parse(value))}close(){}open(){this.readyState=1;this.onopen?.()}closeFromNetwork(){this.readyState=3;this.onclose?.({code:1006})} }
const document={visibilityState:'visible',getElementById:id=>elements.get(id),createElement:id=>new Element(id),addEventListener:(type,fn)=>documentListeners.set(type,fn)};
const navigator={audioSession:{type:'ambient'},mediaSession:{playbackState:'none',metadata:null,setActionHandler:(type,fn)=>actions.set(type,fn)}};
class MediaMetadata { constructor(value){Object.assign(this,value)} }
const context={document,window:{addEventListener:(type,fn)=>windowListeners.set(type,fn)},navigator,MediaMetadata,location:{protocol:'https:',host:'test.invalid'},fetch:async()=>({ok:true,json:async()=>({csrf:'test',state:{generation:1,thresholdTenths:-1000,ctcssTenths:1622,profileId:'lab-demo-hill'},channels:[{profileId:'lab-demo-hill',number:'371',displayName:'Demo Hill',planId:'network-control',planName:'Demo location 52',planGroup:'Other approved categories'}],channel:{status:'applied'}})}),crypto:{randomUUID:()=> '10000000-0000-4000-8000-000000000001'},AudioContext:Context,AudioWorkletNode:Node,WebSocket:Socket,DataView,Number,JSON,BigInt,performance:{now:()=>0},setTimeout:(fn,delay)=>{const id=nextTimer++;timers.set(id,{fn,delay});return id},clearTimeout:id=>timers.delete(id),__T28_TEST_INSTRUMENTATION__:null}; context.globalThis=context;
vm.runInNewContext(portal,context);
const flush=()=>new Promise(resolve=>setImmediate(resolve));
(async()=>{
  await elements.get('play').onclick(); await flush(); const first=Socket.instances[0]; first.open();
  assert.equal(navigator.audioSession.type,'playback','Play configures supported iOS audio-session playback semantics');
  assert.equal(navigator.mediaSession.metadata.title,'T28 Live Audio','Media Session metadata is generic and contains no channel detail');
  assert.equal(navigator.mediaSession.playbackState,'playing','Play publishes truthful active media-session state');
  assert.ok(actions.has('play')&&actions.has('pause'),'Media Session binds both truthful controls');
  const sentBeforeHide=first.sent.length; document.visibilityState='hidden'; documentListeners.get('visibilitychange')(); windowListeners.get('pagehide')();
  assert.equal(first.sent.length,sentBeforeHide,'hide/pagehide is not treated as Pause and does not tear down the v2 transport');
  first.closeFromNetwork(); assert.match(elements.get('status').textContent,/retaining playback intent|reconnecting live audio/i,'an interrupted background socket retains user playback intent: '+elements.get('status').textContent); assert.equal(elements.get('status').dataset.state,'reconnecting','an interrupted transport reports a compact Reconnecting state');
  const recovery=[...timers.values()].find(x=>x.delay===250); assert.ok(recovery,'recovery is bounded'); recovery.fn(); await flush();
  assert.equal(Socket.instances.length,2,'a closed transport is recovered only after close, with no duplicate concurrent socket');
  const second=Socket.instances[1]; second.open(); assert.equal(elements.get('status').textContent,'Connected','recovered transport hides the internal four-packet priming detail'); assert.equal(elements.get('status').dataset.state,'live','Connected retains the Live status-chip state');
  document.visibilityState='visible'; documentListeners.get('visibilitychange')(); await flush();
  assert.equal(Socket.instances.length,2,'foreground resume reuses the recovered transport');
  assert.equal(second.sent.filter(x=>x.type==='credit').at(-1).packets,4,'foreground recovery remains bounded to four startup packets');
  actions.get('pause')(); await flush(); assert.equal(navigator.mediaSession.playbackState,'paused','system pause matches portal pause state'); assert.equal(elements.get('play').textContent,'Play','system pause updates portal control truthfully'); assert.equal(elements.get('status').dataset.state,'paused','system pause reports a compact Paused state');
  actions.get('play')(); await flush(); assert.equal(navigator.mediaSession.playbackState,'playing','system play restores portal playback intent'); assert.equal(elements.get('play').textContent,'Pause','system play restores portal control truthfully');
  console.log('v2 portal background lifecycle: passed (synthetic only; not physically/audibly verified)');
})().catch(error=>{console.error(error.stack||error);process.exitCode=1});

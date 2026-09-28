#!/usr/bin/env node
// Deterministic DOM/lifecycle proof for approved channel switching only.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(`${__dirname}/T28.V2.Server/Portal.cs`, 'utf8');
const portal = source.match(/<script>([\s\S]*?)<\/script>/)?.[1];
assert.ok(portal, 'served v2 portal script is present');
assert.match(source, /<details id=admin class=secondary><summary>Channel controls<\/summary>/, 'Channel controls remains closed by default');
assert.match(source, /<label>Channel plan <select id=plan disabled><\/select><\/label><label>Channel <select id=channel disabled><\/select><\/label>/, 'plan and channel selectors have concise accessible labels');

const elements = new Map();
class Element {
  constructor(id) { this.id=id;this.value=id==='volume'?'1':id==='ctcss'?'1622':'-17';this.textContent=id==='play'?'Play':'';this.innerHTML='';this.dataset={};this.children=[];this.disabled=false; }
  replaceChildren(...children) { this.children=children; }
}
for (const id of ['play','mute','volume','squelch','ctcss','plan','channel','apply','control-error','admin-form','squelch-status','channel-status','channel-name','channel-state','status']) elements.set(id,new Element(id));
const document={visibilityState:'visible',getElementById:id=>elements.get(id),createElement:id=>new Element(id),addEventListener:()=>{}};
class Context { constructor(){this.destination={};this.state='suspended';this.audioWorklet={addModule:async()=>{}}}createGain(){return {gain:{value:1},connect:x=>x}}resume(){this.state='running';return Promise.resolve()}suspend(){this.state='suspended';return Promise.resolve()} }
class Node { constructor(){this.port={postMessage:()=>{}}}connect(x){return x} }
class Socket {
  static instances=[];
  constructor(){this.readyState=0;this.sent=[];Socket.instances.push(this)}
  send(value){this.sent.push(JSON.parse(value))}
  open(){this.readyState=1;this.onopen?.()}
  close(code,reason){this.readyState=3;this.closed={code,reason};this.onclose?.({code,reason})}
}
const networkControl=[['6','Demo location 13'],['24','Demo location 4'],['25','Demo location 11'],['30','Demo location 3'],['52','Demo location 8'],['53','Demo location 61'],['55','Demo Hill'],['56','Demo location 54'],['57','Demo location 55'],['58','Demo location 65'],['77','Demo location 60'],['80','Demo location 12'],['92','Demo location 10'],['105','Demo location 3'],['205','Demo location 9'],['207','Demo location 11'],['217','Demo location 54'],['220','Demo location 12'],['246','Demo location 13'],['263','Demo location 60'],['317','Demo location 10'],['328','Demo location 4'],['371','Demo Hill'],['373','Demo location 8'],['376','Demo location 65'],['377','Demo location 61'],['379','Demo location 5'],['380','Demo location 55']];
const channels=[...networkControl.map(([number,displayName])=>({profileId:number==='371'?'lab-demo-hill':`opaque-network-${number}`,number,displayName,planId:'network-control',planName:'Demo location 52',planGroup:'Other approved categories'})),{profileId:'synthetic-b',number:'TST',displayName:'Synthetic B',planId:'synthetic-plan',planName:'Synthetic plan',planGroup:'Test cell'}];
let active={generation:1,thresholdTenths:-1000,ctcssTenths:1622,profileId:'lab-demo-hill'};
let uuid=0, channelHandler;
const channelCalls=[];
const context={document,window:{addEventListener:()=>{}},location:{protocol:'https:',host:'test.invalid'},fetch:async(path,options)=>{
  if(path==='/v2/session')return {ok:true,json:async()=>({csrf:'test',state:active,channels,channel:{status:'applied'}})};
  if(path==='/v2/channel'){const request=JSON.parse(options.body);channelCalls.push(request);return channelHandler(request)};
  throw new Error('unexpected request');
},crypto:{randomUUID:()=>`10000000-0000-4000-8000-${String(++uuid).padStart(12,'0')}`},AudioContext:Context,AudioWorkletNode:Node,WebSocket:Socket,DataView,Number,JSON,BigInt,setTimeout,clearTimeout,__T28_TEST_INSTRUMENTATION__:null};
context.globalThis=context;
vm.runInNewContext(portal,context);
const flush=()=>new Promise(resolve=>setImmediate(resolve));
const response=(ok,state,profile,reason)=>({ok,json:async()=>({requestId:channelCalls.at(-1).requestId,status:ok?'applied':'rejected',reason,state,activeChannel:channels.find(value=>value.profileId===profile)})});

(async()=>{
  await flush();await flush();
  assert.equal(elements.get('channel-name').textContent,'371 - Demo Hill','the authoritative channel identity is in the card header');
  assert.equal(elements.get('channel-state').textContent,'Active');
  assert.deepEqual(elements.get('plan').children.map(option=>option.value),['network-control','synthetic-plan'],'plan selector preserves server order');assert.deepEqual(elements.get('channel').children.map(option=>[option.value,option.textContent]),networkControl.map(([number,name])=>[number==='371'?'lab-demo-hill':`opaque-network-${number}`,`${number} - ${name}`]),'Demo location 52 filters exactly the numeric ordered map catalogue');assert.equal(elements.get('channel').value,'lab-demo-hill','receiver-authoritative 371 survives full catalogue hydration');

  await elements.get('play').onclick();await flush();const firstSocket=Socket.instances[0];firstSocket.open();
  let resolveApplied,networkAttempt=0;channelHandler=request=>{networkAttempt++;if(networkAttempt===1)throw new Error('synthetic lost response');return new Promise(resolve=>resolveApplied=()=>{active={generation:2,thresholdTenths:-1000,ctcssTenths:1622,profileId:'synthetic-b'};resolve(response(true,active,'synthetic-b'))})};
  elements.get('plan').value='synthetic-plan';elements.get('plan').onchange();elements.get('channel').value='synthetic-b';assert.equal(channelCalls.length,0,'draft selection does not retune');await elements.get('admin-form').onsubmit({preventDefault(){}});await flush();await flush();
  assert.equal(elements.get('channel-state').textContent,'Changing','pending switch is projected truthfully');
  assert.equal(elements.get('channel').disabled,true,'selector is bounded while the owner outcome is pending');
  assert.equal(channelCalls.length,2,'one bounded transport retry was made');
  assert.equal(channelCalls[0].requestId,channelCalls[1].requestId,'transport retry retained one idempotent request identity');
  assert.deepEqual(Object.keys(channelCalls[0]).sort(),['expectedGeneration','profileId','requestId'],'browser submits no tuning or child inputs');
  resolveApplied();await flush();await flush();
  assert.equal(elements.get('channel-name').textContent,'TST - Synthetic B');assert.equal(elements.get('channel-state').textContent,'Active');
  assert.deepEqual(firstSocket.closed,{code:1000,reason:'receiver-generation'},'old browser attempt is explicitly retired');
  assert.equal(Socket.instances.length,2,'retained Play intent reconnects to the promoted audio generation without another gesture');

  const secondSocket=Socket.instances[1];secondSocket.open();await elements.get('play').onclick();assert.equal(elements.get('status').textContent,'Paused');
  channelHandler=async()=>{active={generation:3,thresholdTenths:-1000,ctcssTenths:1622,profileId:'lab-demo-hill'};return response(true,active,'lab-demo-hill')};
  elements.get('plan').value='network-control';elements.get('plan').onchange();elements.get('channel').value='lab-demo-hill';await elements.get('admin-form').onsubmit({preventDefault(){}});await flush();await flush();
  assert.equal(elements.get('channel-name').textContent,'371 - Demo Hill');assert.equal(Socket.instances.length,2,'Pause remains paused across a successful channel switch');assert.equal(elements.get('status').textContent,'Paused');

  channelHandler=async()=>response(false,active,'lab-demo-hill','replacement-failed');
  elements.get('plan').value='synthetic-plan';elements.get('plan').onchange();elements.get('channel').value='synthetic-b';await elements.get('admin-form').onsubmit({preventDefault(){}});await flush();await flush();
  assert.equal(elements.get('channel-state').textContent,'Rejected','sanitized rejection is distinct from active and changing states');
  assert.equal(elements.get('channel-name').textContent,'371 - Demo Hill','rejection keeps the truthful active identity');
  assert.equal(elements.get('channel').value,'lab-demo-hill','rejection restores the approved active selection');
  assert.equal(Socket.instances.length,2,'rejected switch does not create an audio transport');
  console.log('v2 portal approved-channel switching: passed (synthetic only; not physically/audibly verified)');
})().catch(error=>{console.error(error.stack||error);process.exitCode=1});

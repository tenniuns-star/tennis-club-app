import { useState, useRef, useEffect, useCallback } from "react";

// ─── Supabase 설정 ───────────────────────────────────────────
const SUPABASE_URL = "https://yhzgwevuyozksqhvsxwr.supabase.co";
const SUPABASE_KEY = "sb_publishable_4Z-KqglQQwc4tikuSzU0jg_tU2B5WFI";

async function sbFetch(path: string, options: RequestInit = {}) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      "apikey": SUPABASE_KEY,
      "Authorization": `Bearer ${SUPABASE_KEY}`,
      "Prefer": "return=representation",
      ...(options.headers || {}),
    },
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(err);
  }
  const text = await res.text();
  return text ? JSON.parse(text) : [];
}

// ─── Supabase Realtime 웹소켓 ────────────────────────────────
class SupabaseRealtime {
  private ws: WebSocket | null = null;
  private heartbeatInterval: ReturnType<typeof setInterval> | null = null;
  private reconnectTimeout: ReturnType<typeof setTimeout> | null = null;
  private listeners: Map<string, (payload: any) => void> = new Map();
  private connected = false;
  private shouldReconnect = true;
  private ref = 1;

  connect(onConnect?: () => void) {
    this.shouldReconnect = true;
    const wsUrl = `${SUPABASE_URL.replace("https://", "wss://")}/realtime/v1/websocket?apikey=${SUPABASE_KEY}&vsn=1.0.0`;
    this.ws = new WebSocket(wsUrl);

    this.ws.onopen = () => {
      this.connected = true;
      // heartbeat
      this.heartbeatInterval = setInterval(() => {
        this.send({ topic: "phoenix", event: "heartbeat", payload: {}, ref: String(this.ref++) });
      }, 20000);
      onConnect?.();
    };

    this.ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.event === "phx_reply" || msg.event === "heartbeat") return;
        if (msg.event === "postgres_changes") {
          const table = msg.payload?.data?.table;
          if (table) {
            this.listeners.get(table)?.(msg.payload?.data);
          }
        }
      } catch {}
    };

    this.ws.onclose = () => {
      this.connected = false;
      if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
      if (this.shouldReconnect) {
        this.reconnectTimeout = setTimeout(() => this.connect(onConnect), 3000);
      }
    };

    this.ws.onerror = () => {
      this.ws?.close();
    };
  }

  private send(msg: object) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  subscribe(table: string, callback: (payload: any) => void) {
    this.listeners.set(table, callback);
    const topic = `realtime:public:${table}`;
    this.send({
      topic,
      event: "phx_join",
      payload: {
        config: {
          broadcast: { self: false },
          postgres_changes: [{ event: "*", schema: "public", table }],
        },
      },
      ref: String(this.ref++),
    });
  }

  disconnect() {
    this.shouldReconnect = false;
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
    if (this.reconnectTimeout) clearTimeout(this.reconnectTimeout);
    this.ws?.close();
  }
}
// ─────────────────────────────────────────────────────────────

const DEFAULT_MEMBERS = ["조혜경", "김신영", "이화정", "김나나", "한혜준"];
const DEFAULT_CLUB_NAME = "테베럽";
const DEFAULT_CLUB_SUBTITLE = "📍 휘경여자중학교 하드코트 &nbsp;·&nbsp; 🕘 매주 일요일 09:00–12:00";
const TABS = ["세션 구성", "대진표", "통계", "게스트", "관리자"];

const C = {
  teal:"#0D9488",tealL:"#CCFBF1",tealD:"#0F766E",
  coral:"#E85D3A",coralL:"#FEE2D5",coralD:"#C04A2A",
  purple:"#7C3AED",purpleL:"#EDE9FE",purpleD:"#5B21B6",
  amber:"#D97706",amberL:"#FEF3C7",amberD:"#B45309",
  blue:"#2563EB",blueL:"#DBEAFE",blueD:"#1D4ED8",
  gray:"#6B7280",grayL:"#F3F4F6",grayD:"#374151",
  pink:"#DB2777",pinkL:"#FCE7F3",pinkD:"#9D174D",
  green:"#16A34A",greenL:"#DCFCE7",greenD:"#15803D",
};

function pairKey(p: string[]){return[...p].sort().join(" & ");}
function todayStr(){const d=new Date();return`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}-${String(d.getDate()).padStart(2,"0")}`;}
function monthStr(d?: string){return d?.slice(0,7)||"";}

interface Match{pair1:string[];pair2:string[];}
interface ScoreInput{a:string;b:string;draw:boolean;saved:boolean;}
interface CompletedMatch{pair1:string[];pair2:string[];score1:string;score2:string;winner:string[]|null;draw:boolean;}
interface Session{id?:string;date:string;players:string[];matches:CompletedMatch[];playerStatus:Record<string,string>;note?:string;}
interface Guest{id:number;name:string;visits:string[];phone:string;level:string;note:string;}
interface ActiveSession{date:string;players:string[];matches:Match[];scoreInputs:Record<number,ScoreInput>;playerStatus:Record<string,string>;numGames:number;note:string;}
interface GuestForm{name:string;phone:string;level:string;note:string;}
interface AppConfig{clubName:string;clubSubtitle:string;members:string[];nextSessionNote:string;}

function generateKDK(players:string[],numGames:number,playerStatus:Record<string,string>={}):Match[]{
  function availFor(gi:number){return players.filter(p=>{const st=playerStatus[p]||"";if(st==="late"&&gi===0)return false;if(st==="early"&&gi>=numGames-2)return false;return true;});}
  function combosFor(pl:string[]):Match[]{
    const res:Match[]=[],seen=new Set<string>();
    for(let a=0;a<pl.length;a++)for(let b=a+1;b<pl.length;b++)for(let c=0;c<pl.length;c++)for(let d=c+1;d<pl.length;d++){
      if([a,b].some(x=>[c,d].includes(x)))continue;
      const key=[[pl[a],pl[b]].sort().join(","),[pl[c],pl[d]].sort().join(",")].sort().join("|");
      if(seen.has(key))continue;seen.add(key);res.push({pair1:[pl[a],pl[b]],pair2:[pl[c],pl[d]]});
    }
    return res;
  }
  const pc:Record<string,Record<string,number>>={},oc:Record<string,Record<string,number>>={};
  players.forEach(p=>{pc[p]={};oc[p]={};players.forEach(q=>{pc[p][q]=0;oc[p][q]=0;});});
  const selected:Match[]=[];
  for(let i=0;i<numGames;i++){
    const avail=availFor(i);if(avail.length<4)continue;
    const combos=combosFor(avail);if(!combos.length)continue;
    let best:Match|null=null,bestScore=Infinity;
    combos.forEach(m=>{
      const[a,b]=m.pair1,[c,d]=m.pair2;
      const score=(pc[a][b]||0)+(pc[b][a]||0)+(pc[c][d]||0)+(pc[d][c]||0)+(oc[a][c]||0)+(oc[a][d]||0)+(oc[b][c]||0)+(oc[b][d]||0);
      if(score<bestScore){bestScore=score;best=m;}
    });
    if(!best)continue;
    const bm=best as Match;
    selected.push({pair1:[...bm.pair1],pair2:[...bm.pair2]});
    const[a,b]=bm.pair1,[c,d]=bm.pair2;
    pc[a][b]=(pc[a][b]||0)+1;pc[b][a]=(pc[b][a]||0)+1;pc[c][d]=(pc[c][d]||0)+1;pc[d][c]=(pc[d][c]||0)+1;
    [a,b].forEach(p=>[c,d].forEach(q=>{oc[p][q]=(oc[p][q]||0)+1;oc[q][p]=(oc[q][p]||0)+1;}));
  }
  return selected;
}

function calcParticipation(players:string[],numGames:number,playerStatus:Record<string,string>={}){
  const result:Record<string,number[]>={};
  players.forEach(p=>{result[p]=[];for(let i=0;i<numGames;i++){const st=playerStatus[p]||"";if(st==="late"&&i===0)continue;if(st==="early"&&i>=numGames-2)continue;result[p].push(i);}});
  return result;
}
function emptyScore():ScoreInput{return{a:"",b:"",draw:false,saved:false};}

function PastSessionDetail({session,onBack,onDelete,onSaveNote}:{session:Session;onBack:()=>void;onDelete:()=>void;onSaveNote:(note:string)=>void;}){
  const[localNote,setLocalNote]=useState(session.note||"");
  return(
    <div style={{fontFamily:"system-ui,sans-serif",maxWidth:720,margin:"0 auto",background:"#F9FAFB",minHeight:"100vh"}}>
      <div style={{background:`linear-gradient(135deg,${C.teal},${C.tealD})`,padding:"20px"}}>
        <button onClick={onBack} style={{background:"rgba(255,255,255,0.2)",border:"none",color:"#fff",borderRadius:8,padding:"6px 12px",cursor:"pointer",fontSize:13,marginBottom:12}}>← 뒤로</button>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start"}}>
          <div>
            <p style={{margin:0,fontSize:18,fontWeight:600,color:"#fff"}}>{session.date} 대진표</p>
            <p style={{margin:"4px 0 0",fontSize:12,color:"rgba(255,255,255,0.8)"}}>참가: {session.players.join(", ")} · {session.matches.length}게임</p>
          </div>
          <button onClick={onDelete} style={{background:"rgba(232,93,58,0.8)",border:"none",color:"#fff",borderRadius:8,padding:"6px 12px",cursor:"pointer",fontSize:12}}>🗑 삭제</button>
        </div>
      </div>
      <div style={{padding:16,display:"grid",gap:10}}>
        <div style={{background:"#fff",borderRadius:12,border:"1px solid #E5E7EB",padding:"14px 16px"}}>
          <p style={{margin:"0 0 8px",fontSize:13,fontWeight:600,color:"#111"}}>📝 세션 메모</p>
          <textarea value={localNote} onChange={e=>setLocalNote(e.target.value)} rows={3} style={{width:"100%",fontSize:13,padding:"8px 12px",borderRadius:8,border:"1px solid #D1D5DB",outline:"none",resize:"vertical",fontFamily:"inherit",boxSizing:"border-box"}}/>
          <button onClick={()=>{onSaveNote(localNote);alert("메모 저장됐습니다!");}} style={{marginTop:8,padding:"6px 14px",borderRadius:8,fontSize:13,cursor:"pointer",background:C.teal,color:"#fff",border:"none"}}>저장</button>
        </div>
        {session.matches.map((m,i)=>{
          const wk=m.draw?null:pairKey(m.winner!);
          const p1win=!m.draw&&wk===pairKey(m.pair1);
          const p2win=!m.draw&&wk===pairKey(m.pair2);
          return(
            <div key={i} style={{background:"#fff",border:`1px solid ${m.draw?C.amber:p1win?C.teal:C.coral}`,borderRadius:12,padding:"14px 16px"}}>
              <div style={{display:"flex",justifyContent:"space-between",marginBottom:10}}>
                <span style={{fontSize:12,fontWeight:500,color:C.gray,background:C.grayL,padding:"2px 8px",borderRadius:10}}>게임 {i+1}</span>
                {m.draw?<span style={{fontSize:12,color:C.amberD,fontWeight:500}}>🤝 무승부 {m.score1}:{m.score2}</span>:<span style={{fontSize:12,color:C.tealD,fontWeight:500}}>✓ {m.score1}:{m.score2}</span>}
              </div>
              <div style={{display:"flex",alignItems:"center",gap:10}}>
                <div style={{flex:1,background:p1win?C.tealL:m.draw?C.amberL:C.grayL,borderRadius:8,padding:"10px",textAlign:"center",border:p1win?`1px solid ${C.teal}`:m.draw?`1px solid ${C.amber}`:"none"}}>
                  <p style={{fontSize:13,margin:"0 0 4px",fontWeight:500,color:p1win?C.tealD:m.draw?C.amberD:"#111"}}>{m.pair1.join(" & ")}</p>
                  <p style={{fontSize:22,margin:0,fontWeight:600,color:p1win?C.teal:m.draw?C.amber:C.gray}}>{m.score1}</p>
                </div>
                <span style={{fontSize:13,color:C.gray}}>{m.draw?"🤝":"vs"}</span>
                <div style={{flex:1,background:p2win?C.coralL:m.draw?C.amberL:C.grayL,borderRadius:8,padding:"10px",textAlign:"center",border:p2win?`1px solid ${C.coral}`:m.draw?`1px solid ${C.amber}`:"none"}}>
                  <p style={{fontSize:13,margin:"0 0 4px",fontWeight:500,color:p2win?C.coralD:m.draw?C.amberD:"#111"}}>{m.pair2.join(" & ")}</p>
                  <p style={{fontSize:22,margin:0,fontWeight:600,color:p2win?C.coral:m.draw?C.amber:C.gray}}>{m.score2}</p>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export default function App(){
  const[config,setConfig]=useState<AppConfig>({clubName:DEFAULT_CLUB_NAME,clubSubtitle:DEFAULT_CLUB_SUBTITLE,members:DEFAULT_MEMBERS,nextSessionNote:""});
  const[sessions,setSessions]=useState<Session[]>([]);
  const[guests,setGuests]=useState<Guest[]>([]);
  const[loading,setLoading]=useState(true);
  const[syncStatus,setSyncStatus]=useState<"ok"|"syncing"|"error"|"realtime">("ok");
  const realtimeRef=useRef<SupabaseRealtime|null>(null);

  const MEMBERS=config.members;

  const[tab,setTab]=useState(0);
  const[presentMembers,setPresentMembers]=useState<string[]>([]);
  const[memberStatus,setMemberStatus]=useState<Record<string,string>>({});
  const[quickName,setQuickName]=useState("");
  const[todayGuests,setTodayGuests]=useState<string[]>([]);
  const[guestStatus,setGuestStatus]=useState<Record<string,string>>({});
  const[numGames,setNumGames]=useState(5);
  const[activeSession,setActiveSession]=useState<ActiveSession|null>(null);
  const[statMonth,setStatMonth]=useState("");
  const[profilePlayer,setProfilePlayer]=useState<string|null>(null);
  const[editingGuest,setEditingGuest]=useState<number|null>(null);
  const[guestFormData,setGuestFormData]=useState<GuestForm>({name:"",phone:"",level:"중급",note:""});
  const[showAddGuest,setShowAddGuest]=useState(false);
  const[pastSessionIdx,setPastSessionIdx]=useState<number|null>(null);
  const[sessionDate,setSessionDate]=useState(todayStr());
  const[xlsxReady,setXlsxReady]=useState(false);
  const[showResetConfirm,setShowResetConfirm]=useState(false);
  const[resetConfirmText,setResetConfirmText]=useState("");
  const[newMemberName,setNewMemberName]=useState("");
  const[editingMemberIdx,setEditingMemberIdx]=useState<number|null>(null);
  const[editingMemberVal,setEditingMemberVal]=useState("");
  const[pwaInstallPrompt,setPwaInstallPrompt]=useState<any>(null);
  const[showPwaBtn,setShowPwaBtn]=useState(false);
  const[sessionNoteInput,setSessionNoteInput]=useState("");
  const[realtimeConnected,setRealtimeConnected]=useState(false);

  // ── Supabase 데이터 로드 ──
  useEffect(()=>{
    loadAll();
    const handler=(e:any)=>{e.preventDefault();setPwaInstallPrompt(e);setShowPwaBtn(true);};
    window.addEventListener("beforeinstallprompt",handler);
    if((window as any).XLSX){setXlsxReady(true);}else{
      const s=document.createElement("script");s.src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js";s.onload=()=>setXlsxReady(true);document.head.appendChild(s);
    }
    return()=>window.removeEventListener("beforeinstallprompt",handler);
  },[]);

  // ── Supabase Realtime 연결 ──
  useEffect(()=>{
    const rt=new SupabaseRealtime();
    realtimeRef.current=rt;

    rt.connect(()=>{
      setRealtimeConnected(true);

      // sessions 테이블 구독
      rt.subscribe("sessions",(payload)=>{
        const evt=payload.type;
        if(evt==="INSERT"){
          const s=payload.record;
          setSessions(prev=>{
            // 이미 있으면 skip
            if(prev.some(x=>x.id===s.id))return prev;
            return[...prev,{id:s.id,date:s.date,players:s.players,matches:s.matches,playerStatus:s.player_status||{},note:s.note||""}];
          });
        }else if(evt==="UPDATE"){
          const s=payload.record;
          setSessions(prev=>prev.map(x=>x.id===s.id?{id:s.id,date:s.date,players:s.players,matches:s.matches,playerStatus:s.player_status||{},note:s.note||""}:x));
        }else if(evt==="DELETE"){
          const id=payload.old_record?.id;
          if(id)setSessions(prev=>prev.filter(x=>x.id!==id));
        }
      });

      // guests 테이블 구독
      rt.subscribe("guests",(payload)=>{
        const evt=payload.type;
        if(evt==="INSERT"){
          const g=payload.record;
          setGuests(prev=>{
            if(prev.some(x=>x.id===g.id))return prev;
            return[...prev,{id:g.id,name:g.name,visits:g.visits||[],phone:g.phone||"",level:g.level||"중급",note:g.note||""}];
          });
        }else if(evt==="UPDATE"){
          const g=payload.record;
          setGuests(prev=>prev.map(x=>x.id===g.id?{id:g.id,name:g.name,visits:g.visits||[],phone:g.phone||"",level:g.level||"중급",note:g.note||""}:x));
        }else if(evt==="DELETE"){
          const id=payload.old_record?.id;
          if(id)setGuests(prev=>prev.filter(x=>x.id!==id));
        }
      });

      // config 테이블 구독
      rt.subscribe("config",(payload)=>{
        const evt=payload.type;
        if(evt==="UPDATE"||evt==="INSERT"){
          const c=payload.record;
          setConfig({clubName:c.club_name||DEFAULT_CLUB_NAME,clubSubtitle:c.club_subtitle||DEFAULT_CLUB_SUBTITLE,members:c.members||DEFAULT_MEMBERS,nextSessionNote:c.next_session_note||""});
        }
      });
    });

    return()=>{rt.disconnect();setRealtimeConnected(false);};
  },[]);

  async function loadAll(){
    setLoading(true);
    try{
      const[sessData,guestData,cfgData]=await Promise.all([
        sbFetch("/sessions?order=date.asc"),
        sbFetch("/guests?order=id.asc"),
        sbFetch("/config?id=eq.1"),
      ]);
      setSessions(sessData.map((s:any)=>({id:s.id,date:s.date,players:s.players,matches:s.matches,playerStatus:s.player_status||{},note:s.note||""})));
      setGuests(guestData.map((g:any)=>({id:g.id,name:g.name,visits:g.visits||[],phone:g.phone||"",level:g.level||"중급",note:g.note||""})));
      if(cfgData.length>0){
        const c=cfgData[0];
        setConfig({clubName:c.club_name||DEFAULT_CLUB_NAME,clubSubtitle:c.club_subtitle||DEFAULT_CLUB_SUBTITLE,members:c.members||DEFAULT_MEMBERS,nextSessionNote:c.next_session_note||""});
      }
    }catch(e){console.error(e);alert("데이터 로드 실패. 인터넷 연결을 확인해주세요.");}
    setLoading(false);
  }

  async function saveSession(session:Session){
    setSyncStatus("syncing");
    try{
      const body={date:session.date,players:session.players,matches:session.matches,player_status:session.playerStatus,note:session.note||""};
      const res=await sbFetch("/sessions",{method:"POST",body:JSON.stringify(body),headers:{"Prefer":"return=representation"}});
      // Realtime이 INSERT 이벤트를 받아서 자동으로 state 업데이트하므로
      // 여기서 직접 setSessions하지 않아도 됨 (중복 방지 로직 포함)
      setSessions(prev=>{
        if(prev.some(x=>x.id===res[0]?.id))return prev;
        return[...prev,{...session,id:res[0]?.id}];
      });
      setSyncStatus("ok");
    }catch(e){setSyncStatus("error");alert("저장 실패. 다시 시도해주세요.");}
  }

  async function deleteSessionDB(idx:number){
    if(!confirm(`${sessions[idx].date} 세션을 삭제하시겠습니까?`))return;
    const s=sessions[idx];
    setSyncStatus("syncing");
    try{
      if(s.id)await sbFetch(`/sessions?id=eq.${s.id}`,{method:"DELETE"});
      setSessions(prev=>prev.filter((_,i)=>i!==idx));
      setPastSessionIdx(null);
      setSyncStatus("ok");
    }catch(e){setSyncStatus("error");alert("삭제 실패.");}
  }

  async function updateSessionNote(idx:number,note:string){
    const s=sessions[idx];
    try{
      if(s.id)await sbFetch(`/sessions?id=eq.${s.id}`,{method:"PATCH",body:JSON.stringify({note})});
      setSessions(prev=>prev.map((ss,i)=>i===idx?{...ss,note}:ss));
    }catch(e){alert("메모 저장 실패.");}
  }

  async function saveGuestDB(g:Guest){
    setSyncStatus("syncing");
    try{
      const body={id:g.id,name:g.name,visits:g.visits,phone:g.phone,level:g.level,note:g.note};
      await sbFetch("/guests",{method:"POST",body:JSON.stringify(body),headers:{"Prefer":"resolution=merge-duplicates,return=representation"}});
      setSyncStatus("ok");
    }catch(e){setSyncStatus("error");}
  }

  async function updateGuestDB(g:Guest){
    setSyncStatus("syncing");
    try{
      await sbFetch(`/guests?id=eq.${g.id}`,{method:"PATCH",body:JSON.stringify({name:g.name,visits:g.visits,phone:g.phone,level:g.level,note:g.note})});
      setSyncStatus("ok");
    }catch(e){setSyncStatus("error");alert("게스트 업데이트 실패.");}
  }

  async function deleteGuestDB(id:number){
    if(!confirm("게스트를 삭제하시겠습니까?"))return;
    try{
      await sbFetch(`/guests?id=eq.${id}`,{method:"DELETE"});
      setGuests(prev=>prev.filter(g=>g.id!==id));
    }catch(e){alert("삭제 실패.");}
  }

  async function saveConfigDB(newConfig:AppConfig){
    try{
      await sbFetch("/config?id=eq.1",{method:"PATCH",body:JSON.stringify({club_name:newConfig.clubName,club_subtitle:newConfig.clubSubtitle,members:newConfig.members,next_session_note:newConfig.nextSessionNote,updated_at:new Date().toISOString()})});
    }catch(e){console.error("설정 저장 실패",e);}
  }

  function updateConfig(updater:(prev:AppConfig)=>AppConfig){
    setConfig(prev=>{const next=updater(prev);saveConfigDB(next);return next;});
  }

  const allMonths=[...new Set(sessions.map(s=>monthStr(s.date)))].sort().reverse();
  useEffect(()=>{if(!statMonth&&allMonths.length)setStatMonth(allMonths[0]);},[allMonths.length]);

  const todayPlayers=[...presentMembers,...todayGuests];

  function toggleMember(name:string){
    setPresentMembers(prev=>prev.includes(name)?prev.filter(p=>p!==name):[...prev,name]);
    setMemberStatus(prev=>{const n={...prev};if(prev[name])delete n[name];return n;});
  }
  function setStatus(name:string,val:string,isGuest=false){
    const setter=isGuest?setGuestStatus:setMemberStatus;
    setter(prev=>({...prev,[name]:prev[name]===val?"":val}));
  }
  function addQuickGuest(){
    const name=quickName.trim();if(!name)return;
    const date=todayStr();
    const existing=guests.find(g=>g.name===name);
    if(existing){
      const updated={...existing,visits:[...new Set([...existing.visits,date])]};
      setGuests(prev=>prev.map(g=>g.name===name?updated:g));
      updateGuestDB(updated);
    }else{
      const newG:Guest={id:Date.now(),name,visits:[date],phone:"",level:"중급",note:""};
      setGuests(prev=>[...prev,newG]);
      saveGuestDB(newG);
    }
    if(!todayGuests.includes(name))setTodayGuests(prev=>[...prev,name]);
    setQuickName("");
  }
  function removeGuestToday(name:string){
    setTodayGuests(prev=>prev.filter(n=>n!==name));
    setGuestStatus(prev=>{const n={...prev};delete n[name];return n;});
  }

  function startSession(){
    if(todayPlayers.length<4)return alert("최소 4명이 필요합니다.");
    if(todayPlayers.length>10)return alert("최대 10명까지 가능합니다.");
    const allStatus:Record<string,string>={};
    presentMembers.forEach(m=>{allStatus[m]=memberStatus[m]||"";});
    todayGuests.forEach(g=>{allStatus[g]=guestStatus[g]||"";});
    const matches=generateKDK(todayPlayers,numGames,allStatus);
    const scoreInputs:Record<number,ScoreInput>={};
    matches.forEach((_,i)=>{scoreInputs[i]=emptyScore();});
    setActiveSession({date:sessionDate,players:[...todayPlayers],matches,scoreInputs,playerStatus:allStatus,numGames,note:sessionNoteInput});
    setPastSessionIdx(null);setTab(1);
  }

  function addGame(){
    if(!activeSession)return;
    const nm=generateKDK(activeSession.players,1,activeSession.playerStatus);
    if(!nm.length)return alert("추가 가능한 대진 조합이 없습니다.");
    const idx=activeSession.matches.length;
    setActiveSession(prev=>prev?({...prev,matches:[...prev.matches,nm[0]],scoreInputs:{...prev.scoreInputs,[idx]:emptyScore()},numGames:prev.numGames+1}):null);
  }

  function updateScore(idx:number,field:string,val:string|boolean){
    setActiveSession(prev=>prev?({...prev,scoreInputs:{...prev.scoreInputs,[idx]:{...prev.scoreInputs[idx],[field]:val}}}):null);
  }
  function saveScore(idx:number){
    if(!activeSession)return;
    const s=activeSession.scoreInputs[idx]||emptyScore();
    if(s.a===""||s.b==="")return alert("스코어를 입력해주세요.");
    const isDraw=parseInt(s.a)===parseInt(s.b);
    setActiveSession(prev=>prev?({...prev,scoreInputs:{...prev.scoreInputs,[idx]:{...prev.scoreInputs[idx],draw:isDraw,saved:true}}}):null);
  }
  function unlockScore(idx:number){
    setActiveSession(prev=>prev?({...prev,scoreInputs:{...prev.scoreInputs,[idx]:{...prev.scoreInputs[idx],saved:false}}}):null);
  }
  async function finalizeSession(){
    if(!activeSession)return;
    const completed=activeSession.matches.map((m,i)=>{
      const s=activeSession.scoreInputs[i]||emptyScore();
      if(!s.saved)return null;
      const isDraw=parseInt(s.a)===parseInt(s.b);
      if(isDraw)return{pair1:m.pair1,pair2:m.pair2,score1:s.a,score2:s.b,winner:null,draw:true};
      const aWin=parseInt(s.a)>parseInt(s.b);
      return{pair1:m.pair1,pair2:m.pair2,score1:s.a,score2:s.b,winner:aWin?m.pair1:m.pair2,draw:false};
    }).filter(Boolean)as CompletedMatch[];
    if(!completed.length)return alert("저장된 스코어가 없습니다.");
    const newSession:Session={date:activeSession.date,players:activeSession.players,matches:completed,playerStatus:activeSession.playerStatus,note:activeSession.note};
    await saveSession(newSession);
    setActiveSession(null);setTodayGuests([]);setPresentMembers([]);setMemberStatus({});setGuestStatus({});setSessionNoteInput("");
    alert(`${completed.length}게임 기록이 저장됐습니다!`);setTab(2);
  }

  function getStats(mf:string){
    const filtered=mf?sessions.filter(s=>monthStr(s.date)===mf):sessions;
    const stats:Record<string,{wins:number;draws:number;total:number}>={};
    filtered.forEach(sess=>sess.matches.forEach(m=>{
      const k1=pairKey(m.pair1),k2=pairKey(m.pair2),wk=m.draw?null:pairKey(m.winner!);
      [k1,k2].forEach(k=>{
        if(!stats[k])stats[k]={wins:0,draws:0,total:0};
        stats[k].total++;
        if(m.draw)stats[k].draws++;
        else if(k===wk)stats[k].wins++;
      });
    }));
    return Object.entries(stats).map(([pair,{wins,draws,total}])=>({pair,wins,draws,losses:total-wins-draws,total,rate:Math.round(((wins+draws*0.5)/total)*100)})).sort((a,b)=>b.rate-a.rate);
  }

  function getPlayerProfile(name:string){
    const attended=sessions.filter(s=>s.players.includes(name));
    const lateOrEarly=attended.filter(s=>s.playerStatus&&(s.playerStatus[name]==="late"||s.playerStatus[name]==="early")).length;
    let wins=0,losses=0,draws=0,total=0;
    const pStats:Record<string,{wins:number;draws:number;total:number}>={};
    attended.forEach(sess=>sess.matches.forEach(m=>{
      const inP1=m.pair1.includes(name),inP2=m.pair2.includes(name);
      if(!inP1&&!inP2)return;
      total++;
      const myPair=inP1?m.pair1:m.pair2;
      const partner=myPair.find(p=>p!==name);
      if(partner){
        if(!pStats[partner])pStats[partner]={wins:0,draws:0,total:0};
        pStats[partner].total++;
        if(m.draw){draws++;pStats[partner].draws++;}
        else{const won=pairKey(m.winner!)===pairKey(myPair);if(won){wins++;pStats[partner].wins++;}else losses++;}
      }else{if(m.draw)draws++;else{const won=pairKey(m.winner!)===pairKey(myPair);if(won)wins++;else losses++;}}
    }));
    const partnerList=Object.entries(pStats).map(([partner,{wins:w,draws:d,total:t}])=>({partner,wins:w,draws:d,losses:t-w-d,total:t,rate:Math.round(((w+d*0.5)/t)*100)})).sort((a,b)=>b.total-a.total);
    const sessionHistory=attended.map(s=>({date:s.date,totalGames:s.matches.length,myGames:s.matches.filter(m=>m.pair1.includes(name)||m.pair2.includes(name)).length,status:s.playerStatus?.[name]||""})).reverse();
    return{attended:attended.length,lateOrEarly,wins,losses,draws,total,partnerList,sessionHistory};
  }

  function guestTotalVisits(name:string){return sessions.filter(s=>s.players.includes(name)).length;}

  function exportJson(){
    const data={sessions,guests,config,exportedAt:new Date().toISOString()};
    const blob=new Blob([JSON.stringify(data,null,2)],{type:"application/json"});
    const url=URL.createObjectURL(blob);
    const a=document.createElement("a");a.href=url;a.download=`${config.clubName}_백업_${todayStr()}.json`;a.click();URL.revokeObjectURL(url);
  }

  function exportXlsx(){
    const XLSX=(window as any).XLSX;if(!XLSX)return alert("잠시 후 다시 시도해주세요.");
    const wb=XLSX.utils.book_new();
    const rows=sessions.flatMap(s=>s.matches.map(m=>({날짜:s.date,참가자:s.players.join(", "),페어A:m.pair1.join(" & "),페어B:m.pair2.join(" & "),스코어A:m.score1,스코어B:m.score2,결과:m.draw?"무승부":m.winner?m.winner.join(" & ")+"팀 승":"",메모:s.note||""})));
    XLSX.utils.book_append_sheet(wb,XLSX.utils.json_to_sheet(rows),"게임 결과");
    XLSX.utils.book_append_sheet(wb,XLSX.utils.json_to_sheet(getStats("").map((s,i)=>({순위:i+1,페어:s.pair,승:s.wins,무:s.draws,패:s.losses,총게임:s.total,승률:s.rate+"%"}))),"페어 승률");
    XLSX.utils.book_append_sheet(wb,XLSX.utils.json_to_sheet(sessions.map(s=>({날짜:s.date,참가자:s.players.join(", "),게임수:s.matches.length,메모:s.note||""}))),"세션 목록");
    XLSX.utils.book_append_sheet(wb,XLSX.utils.json_to_sheet(guests.map(g=>({이름:g.name,레벨:g.level,전화번호:g.phone,메모:g.note,총방문:guestTotalVisits(g.name)+"회"}))),"게스트 목록");
    XLSX.writeFile(wb,`${config.clubName}_기록_${todayStr()}.xlsx`);
  }

  async function saveGuestForm(){
    if(!guestFormData.name.trim())return alert("이름을 입력해주세요.");
    if(editingGuest){
      const updated=guests.find(g=>g.id===editingGuest);
      if(updated){const g={...updated,...guestFormData};setGuests(prev=>prev.map(x=>x.id===editingGuest?g:x));await updateGuestDB(g);}
    }else{
      const newG:Guest={id:Date.now(),visits:[],...guestFormData};
      setGuests(prev=>[...prev,newG]);await saveGuestDB(newG);
    }
    setShowAddGuest(false);setEditingGuest(null);
  }
  function openEditGuest(g:Guest){setGuestFormData({name:g.name,phone:g.phone||"",level:g.level||"중급",note:g.note||""});setEditingGuest(g.id);setShowAddGuest(true);}
  function doInstallPwa(){if(!pwaInstallPrompt)return;pwaInstallPrompt.prompt();pwaInstallPrompt.userChoice.then(()=>{setPwaInstallPrompt(null);setShowPwaBtn(false);});}

  const allStats=getStats("");
  const monthStats=getStats(statMonth);
  const savedCount=activeSession?Object.values(activeSession.scoreInputs).filter(s=>s.saved).length:0;
  const sortedGuests=[...guests].sort((a,b)=>guestTotalVisits(b.name)-guestTotalVisits(a.name));

  function ParticipationSummary(){
    if(!activeSession)return null;
    const{players,numGames:ng,playerStatus}=activeSession;
    const participation=calcParticipation(players,ng,playerStatus||{});
    const circled=["①","②","③","④","⑤","⑥","⑦","⑧","⑨","⑩"];
    return(
      <div style={{background:"#fff",border:"1px solid #E5E7EB",borderRadius:12,padding:"12px 16px",marginBottom:14}}>
        <p style={{margin:"0 0 10px",fontSize:13,fontWeight:600,color:"#111"}}>📋 인원별 참여 게임</p>
        <div style={{display:"grid",gap:6}}>
          {players.map(p=>{
            const st=playerStatus?.[p]||"";const participates=participation[p]||[];
            return(<div key={p} style={{display:"flex",alignItems:"center",gap:8,flexWrap:"wrap"}}>
              <span style={{fontSize:13,fontWeight:500,color:"#111",minWidth:52}}>{p}</span>
              {st==="late"&&<span style={{fontSize:10,background:C.amberL,color:C.amberD,padding:"1px 5px",borderRadius:4}}>늦참</span>}
              {st==="early"&&<span style={{fontSize:10,background:C.pinkL,color:C.pinkD,padding:"1px 5px",borderRadius:4}}>일퇴</span>}
              <div style={{display:"flex",gap:4,flexWrap:"wrap"}}>
                {Array.from({length:ng},(_,i)=>{const plays=participates.includes(i);return(<span key={i} style={{fontSize:16,color:plays?C.teal:"#D1D5DB",fontWeight:plays?600:400,opacity:plays?1:0.5}}>{circled[i]||i+1}</span>);})}
              </div>
              <span style={{fontSize:11,color:C.gray,marginLeft:"auto"}}>{participates.length}게임</span>
            </div>);
          })}
        </div>
      </div>
    );
  }

  const Pill=({label,active,onClick,ac=C.teal,al=C.tealL,ad=C.tealD}:{label:string;active:boolean;onClick:()=>void;ac?:string;al?:string;ad?:string})=>(
    <button onClick={onClick} style={{padding:"7px 16px",borderRadius:20,fontSize:14,cursor:"pointer",border:`1.5px solid ${active?ac:"#D1D5DB"}`,background:active?al:"#fff",color:active?ad:C.gray,fontWeight:active?600:400}}>{label}</button>
  );
  const StatusTag=({name,isGuest=false}:{name:string;isGuest?:boolean})=>{
    const st=isGuest?guestStatus[name]:memberStatus[name];
    return(<div style={{display:"flex",gap:4}}>
      {([["late","늦참",C.amber,C.amberL,C.amberD],["early","일퇴",C.pink,C.pinkL,C.pinkD]]as[string,string,string,string,string][]).map(([val,label,ac,al,ad])=>(
        <button key={val} onClick={()=>setStatus(name,val,isGuest)} style={{fontSize:11,padding:"3px 8px",borderRadius:8,cursor:"pointer",border:`1px solid ${st===val?ac:"#E5E7EB"}`,background:st===val?al:"#fff",color:st===val?ad:C.gray,fontWeight:st===val?500:400}}>{label}</button>
      ))}
    </div>);
  };

  if(loading){
    return(
      <div style={{fontFamily:"system-ui,sans-serif",maxWidth:720,margin:"0 auto",minHeight:"100vh",display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",background:"#F9FAFB",gap:16}}>
        <span style={{fontSize:48}}>🎾</span>
        <p style={{fontSize:16,fontWeight:600,color:C.teal}}>테베럽 불러오는 중...</p>
        <p style={{fontSize:13,color:C.gray}}>데이터 동기화 중입니다</p>
      </div>
    );
  }

  if(pastSessionIdx!==null&&tab===1){
    const s=sessions[pastSessionIdx];
    if(!s){setPastSessionIdx(null);return null;}
    return(<PastSessionDetail session={s} onBack={()=>setPastSessionIdx(null)} onDelete={()=>deleteSessionDB(pastSessionIdx)} onSaveNote={note=>updateSessionNote(pastSessionIdx,note)}/>);
  }

  if(profilePlayer){
    const isMember=MEMBERS.includes(profilePlayer);
    const p=getPlayerProfile(profilePlayer);
    const gObj=!isMember?guests.find(g=>g.name===profilePlayer):null;
    return(
      <div style={{fontFamily:"system-ui,sans-serif",maxWidth:720,margin:"0 auto",background:"#F9FAFB",minHeight:"100vh"}}>
        <div style={{background:`linear-gradient(135deg,${C.teal},${C.tealD})`,padding:"20px"}}>
          <button onClick={()=>setProfilePlayer(null)} style={{background:"rgba(255,255,255,0.2)",border:"none",color:"#fff",borderRadius:8,padding:"6px 12px",cursor:"pointer",fontSize:13,marginBottom:12}}>← 뒤로</button>
          <div style={{display:"flex",alignItems:"center",gap:14}}>
            <div style={{width:56,height:56,borderRadius:"50%",background:"rgba(255,255,255,0.25)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:22,fontWeight:700,color:"#fff"}}>{profilePlayer[0]}</div>
            <div><h2 style={{margin:0,fontSize:20,fontWeight:600,color:"#fff"}}>{profilePlayer}</h2><p style={{margin:"2px 0 0",fontSize:12,color:"rgba(255,255,255,0.8)"}}>{!isMember?"게스트":"정회원"}</p></div>
          </div>
        </div>
        <div style={{padding:16,display:"grid",gap:14}}>
          {!isMember&&gObj&&(
            <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
              <div style={{background:C.coral,padding:"10px 16px",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                <span style={{fontSize:14,fontWeight:500,color:"#fff"}}>👤 게스트 정보</span>
                <button onClick={()=>{setTab(3);setProfilePlayer(null);openEditGuest(gObj);}} style={{fontSize:12,padding:"4px 10px",borderRadius:8,border:"none",background:"rgba(255,255,255,0.25)",color:"#fff",cursor:"pointer"}}>수정</button>
              </div>
              <div style={{background:"#fff",padding:"14px 16px",display:"grid",gap:6}}>
                {[["연락처",gObj.phone||"없음"],["실력",gObj.level],["메모",gObj.note||"없음"]].map(([l,v])=>(
                  <div key={l} style={{display:"flex",gap:12,fontSize:13}}><span style={{color:C.gray,minWidth:40}}>{l}</span><span style={{color:"#111",whiteSpace:"pre-wrap"}}>{v}</span></div>
                ))}
              </div>
            </div>
          )}
          <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
            <div style={{background:C.blue,padding:"10px 16px"}}><span style={{fontSize:14,fontWeight:500,color:"#fff"}}>📅 참석 현황</span></div>
            <div style={{background:"#fff",padding:"14px 16px"}}>
              <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:10}}>
                {[["총 참석",p.attended+"회"],["늦참/일퇴",p.lateOrEarly+"회"],["게임 참여",p.total+"게임"]].map(([l,v])=>(
                  <div key={l} style={{background:C.blueL,borderRadius:8,padding:"10px 12px"}}><p style={{margin:"0 0 2px",fontSize:11,color:C.blueD}}>{l}</p><p style={{margin:0,fontSize:20,fontWeight:600,color:C.blue}}>{v}</p></div>
                ))}
              </div>
            </div>
          </div>
          <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
            <div style={{background:C.teal,padding:"10px 16px"}}><span style={{fontSize:14,fontWeight:500,color:"#fff"}}>🏆 개인 전적</span></div>
            <div style={{background:"#fff",padding:"14px 16px"}}>
              {p.total===0?<p style={{fontSize:13,color:C.gray}}>게임 기록이 없습니다.</p>:(
                <>
                  <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:8,marginBottom:14}}>
                    {([["승",p.wins,C.teal,C.tealL,C.tealD],["패",p.losses,C.coral,C.coralL,C.coralD],["무",p.draws,C.amber,C.amberL,C.amberD],["승률",Math.round(((p.wins+p.draws*0.5)/p.total)*100)+"%",C.purple,C.purpleL,C.purpleD]]as[string,number|string,string,string,string][]).map(([l,v,ac,al,ad])=>(
                      <div key={l} style={{background:al,borderRadius:8,padding:"10px",textAlign:"center"}}><p style={{margin:"0 0 2px",fontSize:11,color:ad}}>{l}</p><p style={{margin:0,fontSize:18,fontWeight:600,color:ac}}>{v}</p></div>
                    ))}
                  </div>
                  <div style={{height:8,background:"#F3F4F6",borderRadius:4,overflow:"hidden",display:"flex"}}>
                    <div style={{width:`${Math.round((p.wins/p.total)*100)}%`,background:C.teal}}/><div style={{width:`${Math.round((p.draws/p.total)*100)}%`,background:C.amber}}/><div style={{width:`${Math.round((p.losses/p.total)*100)}%`,background:C.coral}}/>
                  </div>
                </>
              )}
            </div>
          </div>
          <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
            <div style={{background:C.purple,padding:"10px 16px"}}><span style={{fontSize:14,fontWeight:500,color:"#fff"}}>🤝 파트너별 승률</span></div>
            <div style={{background:"#fff",padding:"14px 16px"}}>
              {p.partnerList.length===0?<p style={{fontSize:13,color:C.gray}}>데이터가 없습니다.</p>:p.partnerList.map((pt,i)=>(
                <div key={i} style={{marginBottom:12}}>
                  <div style={{display:"flex",justifyContent:"space-between",marginBottom:4}}>
                    <span style={{fontSize:13,fontWeight:500,color:"#111"}}>{pt.partner}</span>
                    <div style={{display:"flex",gap:8,alignItems:"center"}}><span style={{fontSize:11,color:C.gray}}>{pt.total}회 파트너</span><span style={{fontSize:13,fontWeight:500,color:pt.rate>=50?C.tealD:C.coralD}}>{pt.rate}% 승</span></div>
                  </div>
                  <div style={{height:6,background:"#F3F4F6",borderRadius:3,overflow:"hidden"}}><div style={{height:"100%",width:`${pt.rate}%`,background:pt.rate>=50?C.teal:C.coral,borderRadius:3}}/></div>
                  <p style={{margin:"3px 0 0",fontSize:11,color:C.gray}}>{pt.wins}승 {pt.draws}무 {pt.losses}패</p>
                </div>
              ))}
            </div>
          </div>
          <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
            <div style={{background:C.amber,padding:"10px 16px"}}><span style={{fontSize:14,fontWeight:500,color:"#fff"}}>📋 참석 기록</span></div>
            <div style={{background:"#fff",padding:"14px 16px"}}>
              {p.sessionHistory.length===0?<p style={{fontSize:13,color:C.gray}}>기록이 없습니다.</p>:p.sessionHistory.map((s,i)=>(
                <div key={i} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"8px 0",borderBottom:"1px solid #F3F4F6"}}>
                  <span style={{fontSize:13,color:"#111",fontWeight:500}}>{s.date}</span>
                  <div style={{display:"flex",gap:6,alignItems:"center"}}>
                    <span style={{fontSize:12,color:C.gray}}>{s.myGames}게임</span>
                    {s.status==="late"&&<span style={{fontSize:11,background:C.amberL,color:C.amberD,padding:"2px 6px",borderRadius:6}}>늦참</span>}
                    {s.status==="early"&&<span style={{fontSize:11,background:C.pinkL,color:C.pinkD,padding:"2px 6px",borderRadius:6}}>일퇴</span>}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    );
  }

  return(
    <div style={{fontFamily:"system-ui,sans-serif",maxWidth:720,margin:"0 auto",background:"#F9FAFB",minHeight:"100vh"}}>
      {showResetConfirm&&(
        <div style={{position:"fixed",inset:0,background:"rgba(0,0,0,0.5)",zIndex:100,display:"flex",alignItems:"center",justifyContent:"center",padding:16}}>
          <div style={{background:"#fff",borderRadius:16,padding:24,maxWidth:380,width:"100%"}}>
            <p style={{margin:"0 0 8px",fontSize:18,fontWeight:700,color:C.coral}}>⚠️ 스코어 전체 초기화</p>
            <p style={{margin:"0 0 16px",fontSize:14,color:"#374151",lineHeight:1.6}}>모든 세션이 영구 삭제됩니다.<br/><strong style={{color:C.coral}}>"초기화 확인"</strong>을 입력하세요.</p>
            <input value={resetConfirmText} onChange={e=>setResetConfirmText(e.target.value)} placeholder="초기화 확인" style={{width:"100%",fontSize:14,padding:"10px 12px",borderRadius:8,border:`2px solid ${C.coral}`,outline:"none",boxSizing:"border-box",marginBottom:16}}/>
            <div style={{display:"flex",gap:8}}>
              <button onClick={()=>{setShowResetConfirm(false);setResetConfirmText("");}} style={{flex:1,padding:"10px",borderRadius:8,fontSize:14,cursor:"pointer",border:"1px solid #E5E7EB",background:"#fff",color:C.gray}}>취소</button>
              <button disabled={resetConfirmText!=="초기화 확인"} onClick={async()=>{
                try{
                  await sbFetch("/sessions",{method:"DELETE",headers:{"Authorization":`Bearer ${SUPABASE_KEY}`}});
                  setSessions([]);setShowResetConfirm(false);setResetConfirmText("");alert("초기화됐습니다.");
                }catch{alert("초기화 실패.");}
              }} style={{flex:1,padding:"10px",borderRadius:8,fontSize:14,cursor:resetConfirmText==="초기화 확인"?"pointer":"not-allowed",background:resetConfirmText==="초기화 확인"?C.coral:"#E5E7EB",color:resetConfirmText==="초기화 확인"?"#fff":C.gray,border:"none",fontWeight:600}}>초기화 실행</button>
            </div>
          </div>
        </div>
      )}

      {/* 헤더 */}
      <div style={{background:`linear-gradient(135deg,${C.teal},${C.tealD})`,padding:"20px 20px 16px"}}>
        <div style={{display:"flex",alignItems:"flex-start",justifyContent:"space-between"}}>
          <div>
            <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:4}}>
              <span style={{fontSize:24}}>🎾</span>
              <h1 style={{margin:0,fontSize:22,fontWeight:600,color:"#fff"}}>{config.clubName}</h1>
              {/* 실시간 동기화 상태 표시 */}
              {realtimeConnected
                ? <span style={{fontSize:11,background:"rgba(22,163,74,0.4)",color:"#fff",padding:"2px 8px",borderRadius:10,display:"flex",alignItems:"center",gap:4}}>
                    <span style={{width:6,height:6,borderRadius:"50%",background:"#86efac",display:"inline-block"}}/>실시간
                  </span>
                : <span style={{fontSize:11,background:"rgba(255,255,255,0.2)",color:"rgba(255,255,255,0.7)",padding:"2px 8px",borderRadius:10}}>연결 중...</span>
              }
              {syncStatus==="syncing"&&<span style={{fontSize:11,background:"rgba(255,255,255,0.25)",color:"#fff",padding:"2px 8px",borderRadius:10}}>저장 중...</span>}
              {syncStatus==="error"&&<span style={{fontSize:11,background:"rgba(232,93,58,0.5)",color:"#fff",padding:"2px 8px",borderRadius:10}}>⚠️ 오류</span>}
            </div>
            <p style={{margin:0,fontSize:12,color:"rgba(255,255,255,0.85)",lineHeight:1.6}} dangerouslySetInnerHTML={{__html:config.clubSubtitle}}/>
          </div>
          <div style={{textAlign:"right"}}>
            <p style={{margin:0,fontSize:11,color:"rgba(255,255,255,0.7)"}}>정회원</p>
            <p style={{margin:"2px 0 0",fontSize:13,color:"#fff",fontWeight:500}}>{MEMBERS.length}명</p>
          </div>
        </div>
        {showPwaBtn&&<button onClick={doInstallPwa} style={{marginTop:12,padding:"8px 14px",borderRadius:8,fontSize:12,cursor:"pointer",background:"rgba(255,255,255,0.2)",color:"#fff",border:"1px solid rgba(255,255,255,0.4)"}}>📱 홈 화면에 추가</button>}
        {config.nextSessionNote&&<div style={{marginTop:10,background:"rgba(255,255,255,0.15)",borderRadius:8,padding:"8px 12px",fontSize:12,color:"rgba(255,255,255,0.95)"}}>📅 다음 세션: {config.nextSessionNote}</div>}
      </div>

      {/* 탭 */}
      <div style={{background:"#fff",borderBottom:"1px solid #E5E7EB",display:"flex",padding:"0 4px",position:"sticky",top:0,zIndex:10,overflowX:"auto"}}>
        {TABS.map((t,i)=>(
          <button key={i} onClick={()=>{setTab(i);if(i!==1)setPastSessionIdx(null);}} style={{background:"none",border:"none",padding:"12px 10px",cursor:"pointer",fontSize:13,fontWeight:tab===i?600:400,color:tab===i?C.teal:C.gray,borderBottom:tab===i?`2px solid ${C.teal}`:"2px solid transparent",marginBottom:-1,whiteSpace:"nowrap",flexShrink:0}}>
            {t}{i===1&&activeSession?<span style={{marginLeft:4,fontSize:10,background:C.coral,color:"#fff",borderRadius:8,padding:"1px 5px"}}>{savedCount}/{activeSession.matches.length}</span>:null}
          </button>
        ))}
      </div>

      <div style={{padding:16}}>
        {tab===0&&(
          <div style={{display:"grid",gap:14}}>
            <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
              <div style={{background:C.teal,padding:"10px 16px",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                <span style={{fontSize:14,fontWeight:500,color:"#fff"}}>🏅 정회원 출석 체크</span>
                <span style={{fontSize:12,color:"rgba(255,255,255,0.85)"}}>{presentMembers.length}/{MEMBERS.length}명 참석</span>
              </div>
              <div style={{background:"#fff",padding:"14px 16px"}}>
                <p style={{margin:"0 0 10px",fontSize:12,color:C.gray}}>오늘 참석한 회원을 선택해주세요.</p>
                <div style={{display:"grid",gap:10}}>
                  {MEMBERS.map((m,i)=>(
                    <div key={i} style={{display:"flex",alignItems:"center",justifyContent:"space-between",flexWrap:"wrap",gap:6,padding:"6px 0",borderBottom:"1px solid #F9FAFB"}}>
                      <Pill label={m} active={presentMembers.includes(m)} onClick={()=>toggleMember(m)}/>
                      <div style={{display:"flex",alignItems:"center",gap:6}}>
                        <button onClick={()=>setProfilePlayer(m)} style={{fontSize:11,padding:"4px 8px",borderRadius:8,border:"1px solid #E5E7EB",background:"#fff",color:C.gray,cursor:"pointer"}}>프로필</button>
                        {presentMembers.includes(m)&&<StatusTag name={m}/>}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
              <div style={{background:C.coral,padding:"10px 16px",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                <span style={{fontSize:14,fontWeight:500,color:"#fff"}}>👤 오늘 게스트</span>
                <span style={{fontSize:12,color:"rgba(255,255,255,0.85)"}}>{todayGuests.length}명</span>
              </div>
              <div style={{background:"#fff",padding:"14px 16px"}}>
                {todayGuests.length>0&&(
                  <div style={{display:"grid",gap:8,marginBottom:12}}>
                    {todayGuests.map(n=>(
                      <div key={n} style={{display:"flex",alignItems:"center",justifyContent:"space-between",flexWrap:"wrap",gap:6}}>
                        <div style={{display:"flex",alignItems:"center",gap:6,background:C.coralL,border:`1px solid ${C.coral}`,borderRadius:20,padding:"4px 10px 4px 12px"}}>
                          <span style={{fontSize:13,color:C.coralD,fontWeight:500}}>{n}</span>
                          <button onClick={()=>removeGuestToday(n)} style={{background:"none",border:"none",cursor:"pointer",fontSize:14,color:C.coral,lineHeight:1,padding:0}}>×</button>
                        </div>
                        <StatusTag name={n} isGuest={true}/>
                      </div>
                    ))}
                  </div>
                )}
                <div style={{display:"flex",gap:8}}>
                  <input placeholder="게스트 이름 입력 후 Enter" value={quickName} onChange={e=>setQuickName(e.target.value)} onKeyDown={e=>e.key==="Enter"&&addQuickGuest()} style={{flex:1,fontSize:14,padding:"8px 12px",borderRadius:8,border:"1px solid #D1D5DB",outline:"none"}}/>
                  <button onClick={addQuickGuest} style={{padding:"8px 16px",borderRadius:8,fontSize:13,cursor:"pointer",background:C.coral,color:"#fff",border:"none",fontWeight:500}}>추가</button>
                </div>
              </div>
            </div>

            <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
              <div style={{background:C.purple,padding:"10px 16px"}}><span style={{fontSize:14,fontWeight:500,color:"#fff"}}>⚙️ 세션 설정</span></div>
              <div style={{background:"#fff",padding:"14px 16px"}}>
                {(Object.values(memberStatus).some(v=>v)||Object.values(guestStatus).some(v=>v))&&(
                  <div style={{background:C.amberL,borderRadius:8,padding:"8px 12px",marginBottom:12,fontSize:12,color:C.amberD}}>
                    <p style={{margin:"0 0 4px",fontWeight:500}}>⚠️ 특이사항 반영</p>
                    {[...Object.entries(memberStatus),...Object.entries(guestStatus)].filter(([,v])=>v).map(([name,val])=>(
                      <span key={name} style={{marginRight:8,display:"inline-block"}}>{name}: {val==="late"?"늦참 (1게임 제외)":"일퇴 (마지막 2게임 제외)"}</span>
                    ))}
                  </div>
                )}
                <div style={{display:"flex",alignItems:"center",gap:20,flexWrap:"wrap",marginBottom:14}}>
                  <div>
                    <p style={{margin:"0 0 6px",fontSize:13,color:C.gray}}>게임 수</p>
                    <div style={{display:"flex",gap:6}}>
                      {[4,5,6].map(n=>(<button key={n} onClick={()=>setNumGames(n)} style={{width:42,height:38,borderRadius:8,fontSize:14,cursor:"pointer",border:`1.5px solid ${numGames===n?C.purple:"#D1D5DB"}`,background:numGames===n?C.purpleL:"#fff",color:numGames===n?C.purpleD:C.gray,fontWeight:numGames===n?500:400}}>{n}</button>))}
                    </div>
                  </div>
                  <div>
                    <p style={{margin:"0 0 6px",fontSize:13,color:C.gray}}>세션 날짜</p>
                    <input type="date" value={sessionDate} onChange={e=>{if(e.target.value)setSessionDate(e.target.value);}} style={{fontSize:13,fontWeight:500,padding:"4px 8px",borderRadius:8,border:"1px solid #D1D5DB",outline:"none",color:"#111"}}/>
                  </div>
                  <div>
                    <p style={{margin:"0 0 6px",fontSize:13,color:C.gray}}>참가 인원</p>
                    <p style={{margin:0,fontSize:14,fontWeight:500,color:todayPlayers.length>=4&&todayPlayers.length<=10?C.teal:C.coral}}>{todayPlayers.length}명 {todayPlayers.length>=4&&todayPlayers.length<=10?"✓":"⚠️"}</p>
                  </div>
                </div>
                <div style={{marginBottom:14}}>
                  <p style={{margin:"0 0 6px",fontSize:13,color:C.gray}}>📝 세션 메모</p>
                  <textarea value={sessionNoteInput} onChange={e=>setSessionNoteInput(e.target.value)} placeholder="날씨, 코트 상태 등..." rows={2} style={{width:"100%",fontSize:13,padding:"8px 12px",borderRadius:8,border:"1px solid #D1D5DB",outline:"none",resize:"vertical",fontFamily:"inherit",boxSizing:"border-box"}}/>
                </div>
                {todayPlayers.length>=4&&todayPlayers.length<=10&&(
                  <div style={{background:C.tealL,borderRadius:8,padding:"8px 12px",marginBottom:12,fontSize:12,color:C.tealD}}>오늘 참가자: {todayPlayers.join(", ")}</div>
                )}
                <button onClick={startSession} disabled={todayPlayers.length<4||todayPlayers.length>10} style={{padding:"10px 22px",borderRadius:8,fontSize:14,cursor:todayPlayers.length>=4&&todayPlayers.length<=10?"pointer":"not-allowed",background:todayPlayers.length>=4&&todayPlayers.length<=10?C.teal:"#E5E7EB",color:todayPlayers.length>=4&&todayPlayers.length<=10?"#fff":C.gray,border:"none",fontWeight:500}}>🎾 대진표 생성</button>
                {todayPlayers.length<4&&<p style={{margin:"8px 0 0",fontSize:12,color:C.coral}}>최소 4명 이상 선택해주세요.</p>}
                {todayPlayers.length>10&&<p style={{margin:"8px 0 0",fontSize:12,color:C.coral}}>최대 10명까지 가능합니다.</p>}
              </div>
            </div>
          </div>
        )}

        {tab===1&&(
          <div>
            {sessions.length>0&&(
              <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB",marginBottom:14}}>
                <div style={{background:C.gray,padding:"10px 16px",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                  <span style={{fontSize:14,fontWeight:500,color:"#fff"}}>📂 지난 대진표</span>
                  <span style={{fontSize:12,color:"rgba(255,255,255,0.7)"}}>총 {sessions.length}회</span>
                </div>
                <div style={{background:"#fff",padding:"10px 16px",display:"grid",gap:6}}>
                  {[...sessions].reverse().map((s,i)=>{
                    const realIdx=sessions.length-1-i;
                    return(
                      <div key={i} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"8px 12px",borderRadius:8,border:"1px solid #E5E7EB"}}>
                        <button onClick={()=>setPastSessionIdx(realIdx)} style={{flex:1,display:"flex",flexDirection:"column",gap:2,background:"none",border:"none",cursor:"pointer",textAlign:"left",padding:0}}>
                          <div style={{display:"flex",alignItems:"center",gap:8}}>
                            <span style={{fontSize:13,fontWeight:500,color:"#111"}}>{s.date}</span>
                            <span style={{fontSize:11,color:C.gray}}>{s.players.join(", ")}</span>
                          </div>
                          {s.note&&<span style={{fontSize:11,color:C.tealD,background:C.tealL,padding:"1px 6px",borderRadius:4}}>📝 {s.note.slice(0,30)}{s.note.length>30?"...":""}</span>}
                        </button>
                        <div style={{display:"flex",alignItems:"center",gap:6,flexShrink:0}}>
                          <span style={{fontSize:12,color:C.teal,fontWeight:500}}>{s.matches.length}게임</span>
                          <button onClick={()=>deleteSessionDB(realIdx)} style={{fontSize:11,padding:"3px 8px",borderRadius:6,border:"1px solid #FECACA",background:"#FEF2F2",color:C.coral,cursor:"pointer"}}>삭제</button>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
            {!activeSession?(
              <div style={{textAlign:"center",padding:"2rem 0",color:C.gray}}>
                <p style={{fontSize:32,marginBottom:8}}>🎾</p>
                <p style={{fontSize:14}}>세션 구성 탭에서 대진표를 생성해주세요.</p>
              </div>
            ):(
              <>
                <div style={{background:C.tealL,border:`1px solid ${C.teal}`,borderRadius:12,padding:"12px 16px",marginBottom:14,display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                  <div>
                    <p style={{margin:0,fontSize:14,fontWeight:500,color:C.tealD}}>KDK 대진표 · {activeSession.date}</p>
                    <p style={{margin:"2px 0 0",fontSize:12,color:C.teal}}>참가: {activeSession.players.join(", ")}</p>
                    {activeSession.note&&<p style={{margin:"4px 0 0",fontSize:11,color:C.tealD}}>📝 {activeSession.note}</p>}
                  </div>
                  <span style={{fontSize:13,fontWeight:500,color:C.tealD}}>{savedCount}/{activeSession.matches.length} 완료</span>
                </div>
                <ParticipationSummary/>
                <div style={{display:"grid",gap:10,marginBottom:14}}>
                  {activeSession.matches.map((m,i)=>{
                    const s=activeSession.scoreInputs[i]||emptyScore();
                    const pl=activeSession.players;
                    const pairOpts:string[][]=[];
                    for(let a=0;a<pl.length;a++)for(let b=a+1;b<pl.length;b++)pairOpts.push([pl[a],pl[b]]);
                    const drawNow=s.a!==""&&s.b!==""&&parseInt(s.a)===parseInt(s.b);
                    return(
                      <div key={i} style={{background:"#fff",border:`1px solid ${s.saved?(s.draw?C.amber:C.teal):"#E5E7EB"}`,borderRadius:12,padding:"14px 16px"}}>
                        <div style={{display:"flex",justifyContent:"space-between",marginBottom:10}}>
                          <span style={{fontSize:12,fontWeight:500,color:C.gray,background:C.grayL,padding:"2px 8px",borderRadius:10}}>게임 {i+1}</span>
                          {s.saved&&(
                            <div style={{display:"flex",alignItems:"center",gap:8}}>
                              {s.draw?<span style={{fontSize:12,color:C.amberD,fontWeight:500}}>🤝 무승부 {s.a}:{s.b}</span>:<span style={{fontSize:12,color:C.tealD,fontWeight:500}}>✓ {s.a}:{s.b}</span>}
                              <button onClick={()=>unlockScore(i)} style={{fontSize:11,padding:"2px 8px",borderRadius:8,border:`1px solid ${C.teal}`,background:C.tealL,color:C.tealD,cursor:"pointer"}}>✏️ 수정</button>
                            </div>
                          )}
                        </div>
                        <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:10}}>
                          <select value={m.pair1.join("||")} onChange={e=>{const p=e.target.value.split("||");setActiveSession(prev=>{if(!prev)return null;const nm=[...prev.matches];nm[i]={...nm[i],pair1:p};return{...prev,matches:nm};});}} disabled={s.saved} style={{flex:1,fontSize:13,padding:"6px 8px",borderRadius:8,border:"1px solid #D1D5DB",outline:"none",background:s.saved?"#F9FAFB":"#fff"}}>
                            {pairOpts.map(p=><option key={p.join("||")} value={p.join("||")}>{p.join(" & ")}</option>)}
                          </select>
                          <span style={{fontSize:12,color:C.gray}}>vs</span>
                          <select value={m.pair2.join("||")} onChange={e=>{const p=e.target.value.split("||");setActiveSession(prev=>{if(!prev)return null;const nm=[...prev.matches];nm[i]={...nm[i],pair2:p};return{...prev,matches:nm};});}} disabled={s.saved} style={{flex:1,fontSize:13,padding:"6px 8px",borderRadius:8,border:"1px solid #D1D5DB",outline:"none",background:s.saved?"#F9FAFB":"#fff"}}>
                            {pairOpts.map(p=><option key={p.join("||")} value={p.join("||")}>{p.join(" & ")}</option>)}
                          </select>
                        </div>
                        {!s.saved?(
                          <>
                            <div style={{display:"flex",alignItems:"center",gap:8}}>
                              <input type="number" min="0" max="99" placeholder="A" value={s.a} onChange={e=>updateScore(i,"a",e.target.value)} style={{width:64,fontSize:24,padding:"8px",borderRadius:8,border:`1.5px solid ${drawNow?C.amber:C.teal}`,outline:"none",textAlign:"center",fontWeight:700}}/>
                              <span style={{fontSize:drawNow?20:14,color:drawNow?C.amber:C.gray}}>{drawNow?"🤝":":"}</span>
                              <input type="number" min="0" max="99" placeholder="B" value={s.b} onChange={e=>updateScore(i,"b",e.target.value)} style={{width:64,fontSize:24,padding:"8px",borderRadius:8,border:`1.5px solid ${drawNow?C.amber:C.coral}`,outline:"none",textAlign:"center",fontWeight:700}}/>
                              <button onClick={()=>saveScore(i)} style={{flex:1,padding:"10px",borderRadius:8,fontSize:14,cursor:"pointer",background:drawNow?C.amber:C.teal,color:"#fff",border:"none",fontWeight:600}}>저장</button>
                            </div>
                            {drawNow&&<p style={{margin:"6px 0 0",fontSize:11,color:C.amberD,textAlign:"center"}}>🤝 동점 — 저장 시 무승부로 처리됩니다</p>}
                          </>
                        ):(
                          <div style={{display:"flex",gap:8}}>
                            <div style={{flex:1,background:s.draw?C.amberL:parseInt(s.a)>parseInt(s.b)?C.tealL:C.grayL,borderRadius:8,padding:"8px",textAlign:"center",border:s.draw?`1px solid ${C.amber}`:parseInt(s.a)>parseInt(s.b)?`1px solid ${C.teal}`:"none"}}>
                              <p style={{margin:"0 0 2px",fontSize:11,color:C.gray}}>{m.pair1.join(" & ")}</p>
                              <p style={{margin:0,fontSize:24,fontWeight:700,color:s.draw?C.amber:parseInt(s.a)>parseInt(s.b)?C.teal:C.gray}}>{s.a}</p>
                            </div>
                            <div style={{flex:1,background:s.draw?C.amberL:parseInt(s.b)>parseInt(s.a)?C.coralL:C.grayL,borderRadius:8,padding:"8px",textAlign:"center",border:s.draw?`1px solid ${C.amber}`:parseInt(s.b)>parseInt(s.a)?`1px solid ${C.coral}`:"none"}}>
                              <p style={{margin:"0 0 2px",fontSize:11,color:C.gray}}>{m.pair2.join(" & ")}</p>
                              <p style={{margin:0,fontSize:24,fontWeight:700,color:s.draw?C.amber:parseInt(s.b)>parseInt(s.a)?C.coral:C.gray}}>{s.b}</p>
                            </div>
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
                <div style={{display:"flex",gap:8,marginBottom:14}}>
                  <button onClick={addGame} style={{flex:1,padding:"10px",borderRadius:8,fontSize:13,cursor:"pointer",background:C.purpleL,color:C.purpleD,border:`1px solid ${C.purple}`,fontWeight:500}}>+ 게임 추가</button>
                  <button onClick={finalizeSession} disabled={savedCount===0} style={{flex:2,padding:"10px",borderRadius:8,fontSize:13,cursor:savedCount>0?"pointer":"not-allowed",background:savedCount>0?C.teal:"#E5E7EB",color:savedCount>0?"#fff":C.gray,border:"none",fontWeight:600}}>✅ 세션 저장 ({savedCount}게임)</button>
                </div>
              </>
            )}
          </div>
        )}

        {tab===2&&(
          <div style={{display:"grid",gap:14}}>
            <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
              <div style={{background:C.blue,padding:"10px 16px",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                <span style={{fontSize:14,fontWeight:500,color:"#fff"}}>📊 전체 통계 요약</span>
                <button onClick={loadAll} style={{fontSize:12,padding:"4px 10px",borderRadius:8,border:"none",background:"rgba(255,255,255,0.25)",color:"#fff",cursor:"pointer"}}>🔄 새로고침</button>
              </div>
              <div style={{background:"#fff",padding:"14px 16px"}}>
                <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:10}}>
                  {[["세션 수",sessions.length+"회",C.blue,C.blueL],["총 게임",sessions.reduce((a,s)=>a+s.matches.length,0)+"게임",C.teal,C.tealL],["참여 회원",`${MEMBERS.length}명`,C.purple,C.purpleL]].map(([l,v,ac,al])=>(
                    <div key={String(l)} style={{background:String(al),borderRadius:8,padding:"10px 12px"}}>
                      <p style={{margin:"0 0 2px",fontSize:11,color:String(ac)}}>{l}</p>
                      <p style={{margin:0,fontSize:18,fontWeight:600,color:String(ac)}}>{v}</p>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <div style={{borderRadius:12,overflow:"hidden",border:"2px solid "+C.amber,boxShadow:"0 2px 12px rgba(217,119,6,0.12)"}}>
              <div style={{background:`linear-gradient(135deg,${C.amber},${C.amberD})`,padding:"12px 16px",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                <span style={{fontSize:15,fontWeight:700,color:"#fff"}}>🏆 리더보드</span>
                <span style={{fontSize:12,color:"rgba(255,255,255,0.85)"}}>승률 기준 · 클릭 시 프로필</span>
              </div>
              <div style={{background:"#fff",padding:"14px 16px"}}>
                {(()=>{
                  const MEDALS=["🥇","🥈","🥉"];
                  const ranked=[...MEMBERS].map(m=>{const p=getPlayerProfile(m);const rate=p.total>0?Math.round(((p.wins+p.draws*0.5)/p.total)*100):0;return{m,p,rate};}).sort((a,b)=>b.rate===a.rate?b.p.total-a.p.total:b.rate-a.rate);
                  const podium=ranked.slice(0,3);const rest=ranked.slice(3);
                  return(
                    <div>
                      <div style={{display:"flex",alignItems:"flex-end",justifyContent:"center",gap:8,marginBottom:20,marginTop:4}}>
                        {[podium[1],podium[0],podium[2]].filter(Boolean).map((r,i)=>{
                          const isFirst=i===1;const heights=[80,100,70];const bgs=[C.grayL,"#FEF3C7",C.coralL];const borders=["#D1D5DB",C.amber,C.coral];const medals=[MEDALS[1],MEDALS[0],MEDALS[2]];
                          return(
                            <div key={r.m} onClick={()=>setProfilePlayer(r.m)} style={{flex:1,cursor:"pointer",display:"flex",flexDirection:"column",alignItems:"center",gap:6}}>
                              <div style={{fontSize:isFirst?28:22}}>{medals[i]}</div>
                              <div style={{width:isFirst?52:44,height:isFirst?52:44,borderRadius:"50%",background:bgs[i],border:`2px solid ${borders[i]}`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:isFirst?20:16,fontWeight:700,color:"#111"}}>{r.m[0]}</div>
                              <p style={{margin:0,fontSize:13,fontWeight:700,color:"#111",textAlign:"center"}}>{r.m}</p>
                              <p style={{margin:0,fontSize:isFirst?22:18,fontWeight:800,color:r.rate>=50?C.teal:C.coral}}>{r.rate}%</p>
                              <p style={{margin:0,fontSize:10,color:C.gray}}>{r.p.wins}승 {r.p.draws}무 {r.p.losses}패</p>
                              <div style={{width:"100%",height:heights[i],background:bgs[i],borderRadius:"8px 8px 0 0",border:`1px solid ${borders[i]}`,borderBottom:"none"}}/>
                            </div>
                          );
                        })}
                      </div>
                      {rest.length>0&&rest.map((r,i)=>(
                        <div key={r.m} onClick={()=>setProfilePlayer(r.m)} style={{display:"flex",alignItems:"center",gap:12,padding:"10px 12px",borderRadius:10,border:"1px solid #E5E7EB",cursor:"pointer",background:"#FAFAFA",marginBottom:6}}>
                          <span style={{fontSize:13,fontWeight:700,color:C.gray,minWidth:22}}>#{i+4}</span>
                          <div style={{width:34,height:34,borderRadius:"50%",background:C.grayL,display:"flex",alignItems:"center",justifyContent:"center",fontSize:15,fontWeight:700,color:C.grayD}}>{r.m[0]}</div>
                          <div style={{flex:1}}><p style={{margin:0,fontSize:14,fontWeight:600,color:"#111"}}>{r.m}</p><p style={{margin:"2px 0 0",fontSize:11,color:C.gray}}>{r.p.attended}회 참석 · {r.p.total}게임</p></div>
                          <div style={{textAlign:"right"}}><p style={{margin:0,fontSize:16,fontWeight:700,color:r.rate>=50?C.teal:C.coral}}>{r.rate}%</p><p style={{margin:"2px 0 0",fontSize:11,color:C.gray}}>{r.p.wins}승 {r.p.draws}무 {r.p.losses}패</p></div>
                        </div>
                      ))}
                    </div>
                  );
                })()}
              </div>
            </div>

            <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
              <div style={{background:C.purple,padding:"10px 16px",display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}>
                <span style={{fontSize:14,fontWeight:500,color:"#fff"}}>🎯 월별 페어 승률</span>
                <select value={statMonth} onChange={e=>setStatMonth(e.target.value)} style={{fontSize:12,padding:"3px 8px",borderRadius:6,border:"none",background:"rgba(255,255,255,0.25)",color:"#fff",outline:"none"}}>
                  <option value="">전체</option>
                  {allMonths.map(m=><option key={m} value={m}>{m}</option>)}
                </select>
              </div>
              <div style={{background:"#fff",padding:"14px 16px"}}>
                {monthStats.length===0?<p style={{fontSize:13,color:C.gray}}>데이터가 없습니다.</p>:monthStats.slice(0,10).map((s,i)=>(
                  <div key={i} style={{marginBottom:12}}>
                    <div style={{display:"flex",justifyContent:"space-between",marginBottom:4}}>
                      <div style={{display:"flex",alignItems:"center",gap:6}}>
                        <span style={{fontSize:11,background:i===0?C.amberL:C.grayL,color:i===0?C.amberD:C.gray,borderRadius:6,padding:"1px 6px",fontWeight:600}}>#{i+1}</span>
                        <span style={{fontSize:13,fontWeight:500,color:"#111"}}>{s.pair}</span>
                      </div>
                      <span style={{fontSize:13,fontWeight:600,color:s.rate>=50?C.tealD:C.coralD}}>{s.rate}%</span>
                    </div>
                    <div style={{height:6,background:"#F3F4F6",borderRadius:3,overflow:"hidden"}}><div style={{height:"100%",width:`${s.rate}%`,background:s.rate>=50?C.teal:C.coral,borderRadius:3}}/></div>
                    <p style={{margin:"3px 0 0",fontSize:11,color:C.gray}}>{s.wins}승 {s.draws}무 {s.losses}패 · {s.total}게임</p>
                  </div>
                ))}
              </div>
            </div>

            <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
              <div style={{background:C.amber,padding:"10px 16px",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                <span style={{fontSize:14,fontWeight:500,color:"#fff"}}>📅 세션 기록</span>
                <button onClick={exportXlsx} disabled={!xlsxReady} style={{fontSize:12,padding:"4px 10px",borderRadius:8,border:"none",background:"rgba(255,255,255,0.25)",color:"#fff",cursor:"pointer"}}>엑셀 다운로드</button>
              </div>
              <div style={{background:"#fff",padding:"14px 16px"}}>
                {sessions.length===0?<p style={{fontSize:13,color:C.gray}}>기록이 없습니다.</p>:(
                  <div style={{display:"grid",gap:6}}>
                    {[...sessions].reverse().map((s,i)=>{
                      const realIdx=sessions.length-1-i;
                      return(
                        <div key={i} style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",padding:"8px 12px",borderRadius:8,border:"1px solid #E5E7EB"}}>
                          <div><span style={{fontSize:13,fontWeight:500,color:"#111"}}>{s.date}</span><p style={{margin:"2px 0 0",fontSize:11,color:C.gray}}>{s.players.join(", ")} · {s.matches.length}게임</p>{s.note&&<p style={{margin:"2px 0 0",fontSize:11,color:C.tealD}}>📝 {s.note}</p>}</div>
                          <button onClick={()=>deleteSessionDB(realIdx)} style={{fontSize:11,padding:"3px 8px",borderRadius:6,border:"1px solid #FECACA",background:"#FEF2F2",color:C.coral,cursor:"pointer",flexShrink:0}}>삭제</button>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {tab===3&&(
          <div style={{display:"grid",gap:14}}>
            <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
              <div style={{background:C.coral,padding:"10px 16px",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                <span style={{fontSize:14,fontWeight:500,color:"#fff"}}>👤 게스트 목록 ({guests.length}명)</span>
                <button onClick={()=>{setGuestFormData({name:"",phone:"",level:"중급",note:""});setShowAddGuest(true);setEditingGuest(null);}} style={{fontSize:12,padding:"4px 10px",borderRadius:8,border:"none",background:"rgba(255,255,255,0.25)",color:"#fff",cursor:"pointer"}}>+ 추가</button>
              </div>
              <div style={{background:"#fff",padding:"14px 16px"}}>
                {showAddGuest&&(
                  <div style={{background:"#fff",border:`1px solid ${C.teal}`,borderRadius:12,padding:"16px",marginBottom:12}}>
                    <p style={{margin:"0 0 12px",fontSize:14,fontWeight:600,color:"#111"}}>{editingGuest?"게스트 정보 수정":"게스트 추가"}</p>
                    <div style={{display:"grid",gap:8}}>
                      <input placeholder="이름 *" value={guestFormData.name} onChange={e=>setGuestFormData(p=>({...p,name:e.target.value}))} style={{fontSize:14,padding:"8px 12px",borderRadius:8,border:"1px solid #D1D5DB",outline:"none"}}/>
                      <input placeholder="연락처" value={guestFormData.phone} onChange={e=>setGuestFormData(p=>({...p,phone:e.target.value}))} style={{fontSize:14,padding:"8px 12px",borderRadius:8,border:"1px solid #D1D5DB",outline:"none"}}/>
                      <select value={guestFormData.level} onChange={e=>setGuestFormData(p=>({...p,level:e.target.value}))} style={{fontSize:14,padding:"8px 12px",borderRadius:8,border:"1px solid #D1D5DB",outline:"none",background:"#fff"}}>
                        {["초급","중급","고급"].map(l=><option key={l}>{l}</option>)}
                      </select>
                      <textarea placeholder="메모 (특징, 실력, 기타)" value={guestFormData.note} onChange={e=>setGuestFormData(p=>({...p,note:e.target.value}))} rows={3} style={{fontSize:14,padding:"8px 12px",borderRadius:8,border:"1px solid #D1D5DB",outline:"none",resize:"vertical",fontFamily:"inherit"}}/>
                      <div style={{display:"flex",gap:8,justifyContent:"flex-end"}}>
                        <button onClick={()=>{setShowAddGuest(false);setEditingGuest(null);}} style={{padding:"7px 14px",borderRadius:8,fontSize:13,cursor:"pointer",border:"1px solid #E5E7EB",background:"#fff",color:C.gray}}>취소</button>
                        <button onClick={saveGuestForm} style={{padding:"7px 16px",borderRadius:8,fontSize:13,cursor:"pointer",background:C.teal,color:"#fff",border:"none",fontWeight:500}}>저장</button>
                      </div>
                    </div>
                  </div>
                )}
                {sortedGuests.length===0?<p style={{fontSize:13,color:C.gray}}>등록된 게스트가 없습니다.</p>:(
                  <div style={{display:"grid",gap:8}}>
                    {sortedGuests.map(g=>{
                      const visits=guestTotalVisits(g.name);
                      return(
                        <div key={g.id} style={{display:"flex",alignItems:"center",gap:12,padding:"10px 12px",borderRadius:10,border:"1px solid #E5E7EB"}}>
                          <div style={{width:36,height:36,borderRadius:"50%",background:C.coralL,display:"flex",alignItems:"center",justifyContent:"center",fontSize:16,fontWeight:700,color:C.coralD}}>{g.name[0]}</div>
                          <div style={{flex:1,minWidth:0}} onClick={()=>setProfilePlayer(g.name)}>
                            <p style={{margin:0,fontSize:14,fontWeight:600,color:"#111",cursor:"pointer"}}>{g.name} <span style={{fontSize:11,color:C.gray,fontWeight:400}}>({g.level})</span></p>
                            <p style={{margin:"2px 0 0",fontSize:11,color:C.gray}}>{visits}회 참여{g.phone?" · "+g.phone:""}</p>
                            {g.note&&<p style={{margin:"1px 0 0",fontSize:11,color:C.gray,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>📝 {g.note}</p>}
                          </div>
                          <div style={{display:"flex",gap:4}}>
                            <button onClick={()=>openEditGuest(g)} style={{fontSize:11,padding:"4px 8px",borderRadius:6,border:"1px solid #E5E7EB",background:"#fff",color:C.gray,cursor:"pointer"}}>수정</button>
                            <button onClick={()=>deleteGuestDB(g.id)} style={{fontSize:11,padding:"4px 8px",borderRadius:6,border:"1px solid #FECACA",background:"#FEF2F2",color:C.coral,cursor:"pointer"}}>삭제</button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {tab===4&&(
          <div style={{display:"grid",gap:14}}>
            <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
              <div style={{background:C.grayD,padding:"10px 16px"}}><span style={{fontSize:14,fontWeight:500,color:"#fff"}}>⚙️ 클럽 정보 설정</span></div>
              <div style={{background:"#fff",padding:"14px 16px",display:"grid",gap:10}}>
                {[["클럽 이름","clubName"],["클럽 부제 (장소/시간)","clubSubtitle"],["다음 세션 일정 메모","nextSessionNote"]].map(([label,key])=>(
                  <div key={key}>
                    <p style={{margin:"0 0 6px",fontSize:13,color:C.gray}}>{label}</p>
                    <input value={(config as any)[key]} onChange={e=>updateConfig(p=>({...p,[key]:e.target.value}))} style={{width:"100%",fontSize:14,padding:"8px 12px",borderRadius:8,border:"1px solid #D1D5DB",outline:"none",boxSizing:"border-box"}}/>
                  </div>
                ))}
              </div>
            </div>

            <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
              <div style={{background:C.teal,padding:"10px 16px"}}><span style={{fontSize:14,fontWeight:500,color:"#fff"}}>🏅 정회원 관리 ({MEMBERS.length}명)</span></div>
              <div style={{background:"#fff",padding:"14px 16px"}}>
                <div style={{display:"grid",gap:8,marginBottom:12}}>
                  {MEMBERS.map((m,i)=>(
                    <div key={i} style={{display:"flex",alignItems:"center",gap:8,padding:"8px 12px",borderRadius:8,border:"1px solid #E5E7EB"}}>
                      {editingMemberIdx===i?(
                        <>
                          <input value={editingMemberVal} onChange={e=>setEditingMemberVal(e.target.value)} autoFocus style={{flex:1,fontSize:13,padding:"4px 8px",borderRadius:6,border:"1px solid #D1D5DB",outline:"none"}}/>
                          <button onClick={()=>{if(!editingMemberVal.trim())return;updateConfig(p=>({...p,members:p.members.map((mem,j)=>j===i?editingMemberVal.trim():mem)}));setEditingMemberIdx(null);}} style={{fontSize:11,padding:"4px 8px",borderRadius:6,background:C.teal,color:"#fff",border:"none",cursor:"pointer"}}>확인</button>
                          <button onClick={()=>setEditingMemberIdx(null)} style={{fontSize:11,padding:"4px 8px",borderRadius:6,border:"1px solid #E5E7EB",background:"#fff",color:C.gray,cursor:"pointer"}}>취소</button>
                        </>
                      ):(
                        <>
                          <div style={{width:30,height:30,borderRadius:"50%",background:C.tealL,display:"flex",alignItems:"center",justifyContent:"center",fontSize:14,fontWeight:700,color:C.tealD}}>{m[0]}</div>
                          <span style={{flex:1,fontSize:14,fontWeight:500,color:"#111"}}>{m}</span>
                          <button onClick={()=>{setEditingMemberIdx(i);setEditingMemberVal(m);}} style={{fontSize:11,padding:"4px 8px",borderRadius:6,border:"1px solid #E5E7EB",background:"#fff",color:C.gray,cursor:"pointer"}}>수정</button>
                          <button onClick={()=>{if(!confirm(`${m} 회원을 삭제하시겠습니까?`))return;updateConfig(p=>({...p,members:p.members.filter((_,j)=>j!==i)}));}} style={{fontSize:11,padding:"4px 8px",borderRadius:6,border:"1px solid #FECACA",background:"#FEF2F2",color:C.coral,cursor:"pointer"}}>삭제</button>
                        </>
                      )}
                    </div>
                  ))}
                </div>
                <div style={{display:"flex",gap:8}}>
                  <input value={newMemberName} onChange={e=>setNewMemberName(e.target.value)} placeholder="새 회원 이름" onKeyDown={e=>{if(e.key==="Enter"&&newMemberName.trim()){if(MEMBERS.includes(newMemberName.trim()))return alert("이미 등록된 이름입니다.");updateConfig(p=>({...p,members:[...p.members,newMemberName.trim()]}));setNewMemberName("");}}} style={{flex:1,fontSize:14,padding:"8px 12px",borderRadius:8,border:"1px solid #D1D5DB",outline:"none"}}/>
                  <button onClick={()=>{if(!newMemberName.trim())return;if(MEMBERS.includes(newMemberName.trim()))return alert("이미 등록된 이름입니다.");updateConfig(p=>({...p,members:[...p.members,newMemberName.trim()]}));setNewMemberName("");}} style={{padding:"8px 16px",borderRadius:8,fontSize:13,cursor:"pointer",background:C.teal,color:"#fff",border:"none",fontWeight:500}}>추가</button>
                </div>
              </div>
            </div>

            <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
              <div style={{background:C.amber,padding:"10px 16px"}}><span style={{fontSize:14,fontWeight:500,color:"#fff"}}>💾 데이터 관리</span></div>
              <div style={{background:"#fff",padding:"14px 16px",display:"grid",gap:10}}>
                <div style={{display:"flex",gap:8,flexWrap:"wrap"}}>
                  <button onClick={exportJson} style={{padding:"9px 16px",borderRadius:8,fontSize:13,cursor:"pointer",background:C.blueL,color:C.blueD,border:`1px solid ${C.blue}`,fontWeight:500}}>💾 JSON 백업</button>
                  <button onClick={exportXlsx} disabled={!xlsxReady} style={{padding:"9px 16px",borderRadius:8,fontSize:13,cursor:xlsxReady?"pointer":"not-allowed",background:C.greenL,color:C.greenD,border:`1px solid ${C.green}`,fontWeight:500}}>📊 엑셀 내보내기</button>
                  <button onClick={loadAll} style={{padding:"9px 16px",borderRadius:8,fontSize:13,cursor:"pointer",background:C.tealL,color:C.tealD,border:`1px solid ${C.teal}`,fontWeight:500}}>🔄 데이터 새로고침</button>
                </div>
                {/* 실시간 연결 상태 */}
                <div style={{background:realtimeConnected?C.greenL:C.amberL,borderRadius:8,padding:"10px 14px",display:"flex",alignItems:"center",gap:8}}>
                  <span style={{fontSize:18}}>{realtimeConnected?"🟢":"🟡"}</span>
                  <div>
                    <p style={{margin:0,fontSize:13,fontWeight:500,color:realtimeConnected?C.greenD:C.amberD}}>실시간 동기화 {realtimeConnected?"연결됨":"연결 중..."}</p>
                    <p style={{margin:"2px 0 0",fontSize:11,color:realtimeConnected?C.green:C.amber}}>{realtimeConnected?"다른 회원의 변경사항이 자동으로 반영됩니다":"잠시 후 자동으로 연결됩니다"}</p>
                  </div>
                </div>
              </div>
            </div>

            <div style={{borderRadius:12,overflow:"hidden",border:`2px solid ${C.coral}`}}>
              <div style={{background:C.coral,padding:"10px 16px"}}><span style={{fontSize:14,fontWeight:500,color:"#fff"}}>🗑 위험 구역</span></div>
              <div style={{background:"#FEF2F2",padding:"14px 16px"}}>
                <p style={{margin:"0 0 12px",fontSize:13,color:C.coralD}}>아래 작업은 되돌릴 수 없습니다.</p>
                <button onClick={()=>{setShowResetConfirm(true);setResetConfirmText("");}} style={{padding:"10px 20px",borderRadius:8,fontSize:14,cursor:"pointer",background:C.coral,color:"#fff",border:"none",fontWeight:600}}>🗑 스코어 전체 초기화</button>
                <p style={{margin:"8px 0 0",fontSize:11,color:C.coralD}}>현재 {sessions.length}회 세션 · 총 {sessions.reduce((a,s)=>a+s.matches.length,0)}게임</p>
              </div>
            </div>

            {showPwaBtn&&(
              <div style={{borderRadius:12,overflow:"hidden",border:"1px solid #E5E7EB"}}>
                <div style={{background:C.green,padding:"10px 16px"}}><span style={{fontSize:14,fontWeight:500,color:"#fff"}}>📱 앱으로 설치</span></div>
                <div style={{background:"#fff",padding:"14px 16px"}}>
                  <button onClick={doInstallPwa} style={{padding:"10px 20px",borderRadius:8,fontSize:14,cursor:"pointer",background:C.green,color:"#fff",border:"none",fontWeight:600}}>📱 홈 화면에 추가</button>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

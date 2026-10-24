import React, { useCallback, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Activity, Bell, CheckCircle2, Clock3, RefreshCw, Send, TriangleAlert, XCircle } from 'lucide-react';
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import './style.css';

const API = import.meta.env.VITE_API_URL || 'http://localhost:3000';
const icons = { queued: Clock3, processing: Activity, retrying: RefreshCw, sent: CheckCircle2, failed: XCircle };
const labels = { queued: 'Queued', processing: 'Processing', retrying: 'Retrying', sent: 'Delivered', failed: 'Failed' };

function App() {
  const [stats, setStats] = useState(null);
  const [rows, setRows] = useState([]);
  const [busy, setBusy] = useState(null);
  const [notice, setNotice] = useState('');
  const [online, setOnline] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const [statsResponse, listResponse] = await Promise.all([fetch(`${API}/stats`), fetch(`${API}/notifications?limit=30`)]);
      if (!statsResponse.ok || !listResponse.ok) throw new Error('API unavailable');
      setStats(await statsResponse.json());
      setRows((await listResponse.json()).notifications);
      setOnline(true);
    } catch { setOnline(false); }
  }, []);

  useEffect(() => { refresh(); const timer = setInterval(refresh, 2000); return () => clearInterval(timer); }, [refresh]);

  async function retry(id) {
    setBusy(id); setNotice('');
    try {
      const response = await fetch(`${API}/notifications/${id}/retry`, { method: 'POST' });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'Retry failed');
      setNotice('Notification added back to the queue.');
      await refresh();
    } catch (error) { setNotice(error.message); }
    finally { setBusy(null); }
  }

  const counts = stats?.counts || {};
  return <main className="shell">
    <header className="topbar"><a className="brand" href="#top"><span className="brandmark"><Bell size={19}/></span><span>notifyflow<span className="brand-period">.</span></span></a><div className="topmeta"><span className={`live-dot ${online ? '' : 'offline'}`}/><span>{online ? 'System operational' : 'Connecting to API'}</span><button className="icon-button" onClick={refresh} title="Refresh"><RefreshCw size={16}/></button></div></header>
    <section className="intro"><div><div className="eyebrow"><span className="eyebrow-line"/>DELIVERY OVERVIEW</div><h1>Notification center</h1><p>Track delivery health, queue activity, and recent notifications.</p></div><a className="rabbit-link" href="http://localhost:15672" target="_blank" rel="noreferrer"><Activity size={16}/> RabbitMQ <span>↗</span></a></section>
    {notice && <div className="notice" role="status">{notice}<button onClick={() => setNotice('')}>×</button></div>}
    <section className="metrics" aria-label="Notification counts">{['queued', 'processing', 'retrying', 'sent', 'failed'].map((key) => {
      const Icon = icons[key]; return <article className={`metric ${key}`} key={key}><div className="metric-top"><span>{labels[key]}</span><span className="metric-icon"><Icon size={17}/></span></div><strong>{counts[key] ?? '—'}</strong><small>{key === 'sent' ? 'successfully delivered' : key === 'failed' ? 'need attention' : 'in the pipeline'}</small></article>;
    })}</section>
    <section className="content-grid"><article className="panel chart-panel"><div className="panel-heading"><div><h2>Queue activity</h2><p>Notifications accepted · last 30 minutes</p></div><span className="chart-legend"><i/> Accepted</span></div><div className="chart-area">{stats?.throughput?.length ? <ResponsiveContainer width="100%" height="100%"><AreaChart data={stats.throughput} margin={{ top: 10, right: 12, left: -18, bottom: 0 }}><defs><linearGradient id="fillBlue" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#4567e8" stopOpacity={0.2}/><stop offset="95%" stopColor="#4567e8" stopOpacity={0}/></linearGradient></defs><CartesianGrid vertical={false} stroke="#edf0f6"/><XAxis dataKey="time" tickFormatter={(value) => new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} tickLine={false} axisLine={false} tick={{ fill: '#8c95a8', fontSize: 11 }}/><YAxis allowDecimals={false} tickLine={false} axisLine={false} tick={{ fill: '#8c95a8', fontSize: 11 }}/><Tooltip labelFormatter={(value) => new Date(value).toLocaleString()} contentStyle={{ border: '1px solid #e9edf4', borderRadius: 9, boxShadow: '0 8px 24px #25385815' }}/><Area type="monotone" dataKey="count" name="Accepted" stroke="#4567e8" strokeWidth={2.5} fill="url(#fillBlue)"/></AreaChart></ResponsiveContainer> : <div className="empty-chart"><Activity size={20}/><span>Activity will appear as notifications arrive</span></div>}</div></article>
      <article className="panel queue-panel"><div className="panel-heading"><div><h2>Queue depth</h2><p>Messages waiting for a worker</p></div><span className="tiny-live"><i/> LIVE</span></div><div className="queue-list">{[['email','Email'],['sms','SMS'],['push','Push'],['dlq','Dead letter']].map(([key,label])=><div className="queue-row" key={key}><span className={`queue-bullet ${key}`}/><span>{label}</span><strong>{stats?.queues?.[key] ?? '—'}</strong></div>)}</div><div className="queue-foot"><span>Workers consume up to 10 messages each</span></div></article></section>
    <section className="panel table-panel"><div className="panel-heading table-heading"><div><h2>Recent notifications</h2><p>Latest delivery requests across all channels</p></div><span className="row-count">{rows.length} records</span></div><div className="table-wrap"><table><thead><tr><th>RECIPIENT</th><th>CHANNEL</th><th>TEMPLATE</th><th>STATUS</th><th>ATTEMPTS</th><th>CREATED</th><th/></tr></thead><tbody>{rows.length ? rows.map((row) => <tr key={row.id}><td><span className="recipient">{row.recipient}</span><span className="id">{row.id.slice(0,8)}</span></td><td><span className={`channel ${row.channel}`}>{row.channel}</span></td><td>{row.template}</td><td><span className={`status ${row.status}`}><i/>{labels[row.status] || row.status}</span></td><td>{row.attempts}</td><td>{new Date(row.created_at).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}</td><td>{row.status === 'failed' && <button className="retry-button" onClick={() => retry(row.id)} disabled={busy === row.id}><Send size={13}/>{busy === row.id ? 'Retrying' : 'Retry'}</button>}</td></tr>) : <tr><td className="no-rows" colSpan="7"><TriangleAlert size={16}/> No notifications yet. Send one through the API to see it here.</td></tr>}</tbody></table></div></section>
    <footer><span><span className="footer-mark">●</span> NotifyFlow <span className="footer-muted">· Reliable delivery, in the background.</span></span><span>Refreshes every 2 seconds</span></footer>
  </main>;
}

createRoot(document.getElementById('root')).render(<App/>);

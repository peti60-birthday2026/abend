/*
  Gemeinsame Funktionen für die Party-Spiele (Regie, Beamer, Mitspielen).

  Kanäle bei Ably:
    spiele-geburtstag-2026:state  Spielstand von der Regie (der letzte wird beim Öffnen nachgeladen)
    spiele-geburtstag-2026:input  Antworten, Buzzer, Stimmen und Grüße von den Handys
    spiele-geburtstag-2026:files  Fotos, in Stücke zerlegt (Ably erlaubt max. 64 KB pro Nachricht)
*/
(function(){
  const ABLY_KEY = "IUC-Ig.qxmWMQ:chvGoEpWoIoa1DjOt7o2TM5FLzKUhlJqAJTGXAY8ECs";
  const BASE = "spiele-geburtstag-2026";
  const CHUNK = 45000;
  const SPOTIFY_CLIENT_ID = "c1a5d3f7e8334f0ea49b7499c151d8d3";

  const Core = {};
  Core.WISH_CHANNEL = "playlist-geburtstag-2026";

  Core.esc = function(s){
    const d = document.createElement('div');
    d.textContent = s == null ? '' : String(s);
    return d.innerHTML.replace(/"/g, '&quot;');
  };
  Core.uid = function(prefix){
    return (prefix || '') + Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);
  };
  Core.store = {
    get(k, def){
      try{ const v = localStorage.getItem(k); return v == null ? def : JSON.parse(v); }catch(e){ return def; }
    },
    set(k, v){
      try{ localStorage.setItem(k, JSON.stringify(v)); return true; }catch(e){ return false; }
    }
  };
  Core.shuffle = function(arr){
    const a = arr.slice();
    for(let i = a.length - 1; i > 0; i--){ const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; }
    return a;
  };
  Core.nameKey = function(n){ return (n || '').trim().toLowerCase(); };
  // Zahlen deutsch formatieren, Jahreszahlen aber ohne Tausenderpunkt (1970 statt 1.970)
  Core.fmtNum = function(n){
    if(typeof n !== 'number') return Core.esc(n);
    const isYear = Number.isInteger(n) && n >= 1000 && n <= 2999;
    return n.toLocaleString('de-DE', { useGrouping: !isYear, maximumFractionDigits: 2 });
  };

  // ---------- Fotos lokal speichern (IndexedDB) ----------
  let dbP = null;
  function db(){
    if(!dbP) dbP = new Promise((res, rej) => {
      const r = indexedDB.open('spiele-dateien', 1);
      r.onupgradeneeded = () => r.result.createObjectStore('files');
      r.onsuccess = () => res(r.result);
      r.onerror = () => rej(r.error);
    });
    return dbP;
  }
  function tx(mode, fn){
    return db().then(d => new Promise((res) => {
      const t = d.transaction('files', mode);
      const req = fn(t.objectStore('files'));
      t.oncomplete = () => res(req && 'result' in req ? req.result : undefined);
      t.onerror = () => res(undefined);
      t.onabort = () => res(undefined);
    })).catch(() => undefined);
  }
  const memFiles = new Map();
  Core.fileGet = async function(k){
    if(memFiles.has(k)) return memFiles.get(k);
    const v = await tx('readonly', s => s.get(k));
    if(v) memFiles.set(k, v);
    return v || null;
  };
  Core.fileSet = function(k, v){ memFiles.set(k, v); return tx('readwrite', s => s.put(v, k)); };
  Core.fileDel = function(k){ memFiles.delete(k); return tx('readwrite', s => s.delete(k)); };
  Core.fileMem = function(k){ return memFiles.get(k) || null; };

  Core.compressImage = async function(file, max, quality){
    const url = URL.createObjectURL(file);
    try{
      const img = await new Promise((res, rej) => { const i = new Image(); i.onload = () => res(i); i.onerror = rej; i.src = url; });
      const s = Math.min(1, max / Math.max(img.naturalWidth, img.naturalHeight));
      const c = document.createElement('canvas');
      c.width = Math.max(1, Math.round(img.naturalWidth * s));
      c.height = Math.max(1, Math.round(img.naturalHeight * s));
      c.getContext('2d').drawImage(img, 0, 0, c.width, c.height);
      return c.toDataURL('image/jpeg', quality);
    }finally{
      URL.revokeObjectURL(url);
    }
  };

  // ---------- Verbindung ----------
  Core.connect = function(opts){
    opts = opts || {};
    const ably = new Ably.Realtime({ key: ABLY_KEY });
    const ch = {
      state: ably.channels.get(BASE + ':state', { params: { rewind: '1' } }),
      input: ably.channels.get(BASE + ':input'),
      files: ably.channels.get(BASE + ':files')
    };
    const parts = {};
    const done = new Set();

    if(opts.onConnection){
      ably.connection.on((change) => opts.onConnection(change.current));
    }
    if(opts.onState){
      ch.state.subscribe('state', (msg) => { if(msg.data) opts.onState(msg.data); });
    }
    if(opts.onInput){
      ch.input.subscribe((msg) => { if(msg.data) opts.onInput(msg.name, msg.data); });
    }
    if(opts.receiveFiles !== false){
      ch.files.subscribe('chunk', (msg) => {
        const d = msg.data;
        if(!d || !d.id || done.has(d.id) || typeof d.d !== 'string') return;
        const p = parts[d.id] || (parts[d.id] = { n: d.n, got: new Array(d.n), count: 0 });
        if(p.got[d.i] == null){ p.got[d.i] = d.d; p.count++; }
        if(p.count === p.n){
          const data = p.got.join('');
          delete parts[d.id];
          done.add(d.id);
          Core.fileSet(d.id, data).then(() => { if(opts.onFile) opts.onFile(d.id, data); });
        }
      });
    }

    const needAsked = {};
    return {
      ably, ch,
      input(name, data){ ch.input.publish(name, data); },
      async sendFile(id, data, onProgress){
        const n = Math.ceil(data.length / CHUNK);
        for(let i = 0; i < n; i++){
          await new Promise(res => ch.files.publish('chunk', { id, i, n, d: data.slice(i * CHUNK, (i + 1) * CHUNK) }, () => res()));
          if(onProgress) onProgress(i + 1, n);
        }
      },
      // Foto holen: aus dem Speicher, sonst bei der Regie anfordern (kommt dann über onFile)
      async image(id){
        if(!id) return null;
        const v = await Core.fileGet(id);
        if(v) return v;
        if(!needAsked[id] || Date.now() - needAsked[id] > 8000){
          needAsked[id] = Date.now();
          ch.input.publish('need', { id });
        }
        return null;
      }
    };
  };

  // ---------- Spieler-Identität (Handy) ----------
  Core.player = function(){
    let p = Core.store.get('spiele_player', null);
    if(!p || !p.pid){
      const wish = Core.store.get('playlist_wunsch_identity', null);
      p = { pid: Core.uid('p_'), name: (wish && wish.name) || '' };
      Core.store.set('spiele_player', p);
    }
    return p;
  };
  Core.savePlayer = function(p){ Core.store.set('spiele_player', p); };

  // ---------- Spotify: was läuft gerade? (nutzt die Anmeldung der Jetzt-läuft-Anzeige) ----------
  Core.spotifyNowPlaying = async function(){
    let token = Core.store.get('np_sp_token', null);
    if(!token) return { error: 'Auf diesem Gerät ist Spotify nicht verbunden (einmal die Anzeige öffnen und verbinden).' };
    if(Date.now() >= token.exp){
      if(!token.refresh) return { error: 'Spotify-Anmeldung abgelaufen.' };
      const res = await fetch('https://accounts.spotify.com/api/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: token.refresh, client_id: localStorage.getItem('np_sp_client') || SPOTIFY_CLIENT_ID })
      });
      const data = await res.json().catch(() => ({}));
      if(!res.ok || !data.access_token) return { error: 'Spotify-Anmeldung abgelaufen, bitte in der Anzeige neu verbinden.' };
      token = { access: data.access_token, refresh: data.refresh_token || token.refresh, exp: Date.now() + ((data.expires_in || 3600) - 60) * 1000 };
      Core.store.set('np_sp_token', token);
    }
    const res = await fetch('https://api.spotify.com/v1/me/player/currently-playing', { headers: { Authorization: 'Bearer ' + token.access } });
    if(res.status === 204) return { error: 'Bei Spotify läuft gerade nichts.' };
    if(!res.ok) return { error: 'Spotify antwortet nicht (' + res.status + ').' };
    const d = await res.json();
    if(!d.item) return { error: 'Bei Spotify läuft gerade kein Song.' };
    const imgs = (d.item.album && d.item.album.images) || [];
    return {
      title: d.item.name,
      artists: (d.item.artists || []).map(a => a.name).join(', '),
      cover: imgs.length ? imgs[0].url : ''
    };
  };

  // ---------- Vorlagen-Listen (Regie und Druckvorlagen) ----------
  Core.DEFAULT_LISTS = {
    challenge:
`Becher-Pyramide | Baue aus 10 Bechern eine Pyramide und wieder ab.
Ballon-Marathon | Blase 3 Luftballons auf und verknote sie.
Keks-Gesicht | Keks auf die Stirn legen und ohne Hände in den Mund bekommen.
Münzturm | Staple 15 Münzen mit einer Hand übereinander.
Strohhalm-Transport | Bring 10 Smarties mit einem Strohhalm von einem Teller auf den anderen.
Wäscheklammer-Kette | Hänge 15 Wäscheklammern an deinen Ärmel, nur mit einer Hand.
Zungenbrecher | Sag 5-mal fehlerfrei: „Fischers Fritz fischt frische Fische“.
Papierflieger | Falte einen Flieger, der mindestens 3 Meter weit fliegt.
Socken-Sortierer | Sortiere 8 gemischte Sockenpaare, mit verbundenen Augen.
Tischtennis-Hüpfer | Lass einen Tischtennisball einmal aufspringen und in einen Becher hüpfen.
Kartenhaus | Baue ein Kartenhaus mit 2 Etagen.
Nudel-Angeln | Fädle mit einer Spaghetti im Mund 6 Penne auf.
Taschentuch-Zauber | Zieh mit einer Hand alle Taschentücher aus der Box.
Gefühlte Minute | Ohne Uhr: Ruf genau nach 60 Sekunden „Stopp!“.
Reim-Maschine | Finde 10 Wörter, die sich auf „Peti“ reimen.`,
    pantomime:
`Torte anschneiden
Kerzen auspusten
Geschenk auspacken
Polonaise anführen
Im Stau stehen
Disco-Tanz der 80er
Zelt aufbauen
Lesebrille suchen
Fernbedienung suchen
Rasen mähen
Kaffeeklatsch
Am Strand liegen
Selfie machen
Tor bejubeln
Elvis-Hüftschwung
Schallplatte auflegen
Kreuzworträtsel lösen
Wäsche aufhängen
Fahrrad reparieren
Brief schreiben`,
    drawing:
`Geburtstagstorte
Schallplatte
Wählscheiben-Telefon
VW Käfer
Kassettenrekorder
Diskokugel
Gartenzwerg
Wohnwagen
Röhrenfernseher
Luftballon
Sektglas
Mondlandung
Schlaghose
Lockenwickler
Liegestuhl
Rasenmäher
Brille
Geschenk
Fotoapparat
Gitarre`
  };
  Core.lists = function(){
    return Object.assign({}, Core.DEFAULT_LISTS, Core.store.get('spiele_lists', {}));
  };
  Core.settings = function(){
    return Object.assign({ name: 'Peti', title: 'Peti wird 60', birthday: '' }, Core.store.get('spiele_settings', {}));
  };

  // ---------- QR-Code ----------
  Core.qr = function(el, path, size){
    if(!el || typeof QRCode === 'undefined' || !/^https?:$/.test(location.protocol)) return false;
    el.innerHTML = '';
    new QRCode(el, {
      text: new URL(path, location.href).href,
      width: size || 256, height: size || 256,
      colorDark: '#0d0d12', colorLight: '#ffffff',
      correctLevel: QRCode.CorrectLevel.M
    });
    return true;
  };

  window.SpieleCore = Core;
})();

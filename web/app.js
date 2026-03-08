const state = {
  token: localStorage.getItem("nexus.token") || "",
  userId: Number(localStorage.getItem("nexus.userId") || 0),
  ws: null,
};

const dom = {
  baseUrl: document.getElementById("baseUrlInput"),
  authBadge: document.getElementById("authBadge"),
  wsBadge: document.getElementById("wsBadge"),
  registerTab: document.getElementById("registerTab"),
  loginTab: document.getElementById("loginTab"),
  registerForm: document.getElementById("registerForm"),
  loginForm: document.getElementById("loginForm"),
  regUsername: document.getElementById("regUsername"),
  regPassword: document.getElementById("regPassword"),
  regNickname: document.getElementById("regNickname"),
  loginUsername: document.getElementById("loginUsername"),
  loginPassword: document.getElementById("loginPassword"),
  tokenInput: document.getElementById("tokenInput"),
  applyTokenBtn: document.getElementById("applyTokenBtn"),
  convIdInput: document.getElementById("convIdInput"),
  connectBtn: document.getElementById("connectBtn"),
  historyBtn: document.getElementById("historyBtn"),
  messages: document.getElementById("messages"),
  sendForm: document.getElementById("sendForm"),
  messageInput: document.getElementById("messageInput"),
  aiHintBtn: document.getElementById("aiHintBtn"),
  log: document.getElementById("log"),
  clearLogBtn: document.getElementById("clearLogBtn"),
};

dom.tokenInput.value = state.token;
renderAuthBadge();

function setBadge(el, text, cls) {
  el.textContent = text;
  el.classList.remove("muted", "ok", "warn");
  el.classList.add(cls);
}

function renderAuthBadge() {
  if (!state.token || !state.userId) {
    setBadge(dom.authBadge, "Guest", "muted");
    return;
  }
  setBadge(dom.authBadge, `User ${state.userId}`, "ok");
}

function logLine(text) {
  const now = new Date().toLocaleTimeString();
  dom.log.textContent += `[${now}] ${text}\n`;
  dom.log.scrollTop = dom.log.scrollHeight;
}

function baseUrl() {
  return dom.baseUrl.value.trim().replace(/\/$/, "");
}

function wsUrl() {
  return baseUrl().replace(/^http/, "ws") + `/api/v1/ws?token=${encodeURIComponent(state.token)}`;
}

async function request(path, options = {}) {
  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {}),
  };
  if (state.token) {
    headers.Authorization = `Bearer ${state.token}`;
  }

  const res = await fetch(baseUrl() + path, { ...options, headers });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(body.error || `${res.status} ${res.statusText}`);
  }
  return body;
}

function switchTab(mode) {
  const register = mode === "register";
  dom.registerTab.classList.toggle("active", register);
  dom.loginTab.classList.toggle("active", !register);
  dom.registerForm.classList.toggle("hidden", !register);
  dom.loginForm.classList.toggle("hidden", register);
}

function addMessage(m, self) {
  const wrap = document.createElement("article");
  wrap.className = `msg ${self ? "me" : "other"}`;
  const meta = document.createElement("div");
  meta.className = "meta";
  meta.textContent = `sender: ${m.sender_id ?? m.senderID ?? "?"}  msg_id: ${m.msg_id ?? m.msgID ?? "pending"}`;
  const text = document.createElement("div");
  text.textContent = m.content || "";
  wrap.appendChild(meta);
  wrap.appendChild(text);
  dom.messages.appendChild(wrap);
  dom.messages.scrollTop = dom.messages.scrollHeight;
}

function connectWs() {
  if (!state.token) {
    logLine("Missing token. Login first.");
    return;
  }
  if (state.ws && state.ws.readyState === WebSocket.OPEN) {
    state.ws.close();
  }

  const sock = new WebSocket(wsUrl());
  state.ws = sock;
  setBadge(dom.wsBadge, "Connecting", "warn");

  sock.onopen = () => {
    setBadge(dom.wsBadge, "Connected", "ok");
    logLine("WebSocket connected.");
  };

  sock.onclose = () => {
    setBadge(dom.wsBadge, "Disconnected", "muted");
    logLine("WebSocket disconnected.");
  };

  sock.onerror = () => {
    setBadge(dom.wsBadge, "Error", "warn");
    logLine("WebSocket error.");
  };

  sock.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.status === "sending") {
        logLine(`Ack: ${msg.temp_id || "-"}`);
        return;
      }
      addMessage(msg, Number(msg.sender_id || msg.senderID) === state.userId);
    } catch {
      logLine(`WS raw: ${event.data}`);
    }
  };
}

async function loadHistory() {
  const convID = Number(dom.convIdInput.value || 0);
  if (!convID) {
    logLine("Conversation ID is required.");
    return;
  }
  try {
    const data = await request(`/api/v1/messages/${convID}?limit=30`);
    dom.messages.innerHTML = "";
    const list = (data.messages || []).slice().reverse();
    for (const item of list) {
      addMessage(item, Number(item.sender_id || item.senderID) === state.userId);
    }
    logLine(`History loaded from ${data.source || "unknown"}.`);
  } catch (err) {
    logLine(`History failed: ${err.message}`);
  }
}

function makeTempID() {
  return `web-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
}

function sendMessage(event) {
  event.preventDefault();
  const content = dom.messageInput.value.trim();
  const convID = Number(dom.convIdInput.value || 0);
  if (!content || !convID) {
    return;
  }
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
    logLine("WebSocket is not connected.");
    return;
  }

  const payload = {
    temp_id: makeTempID(),
    conversation_id: convID,
    content,
    sent_at: Date.now(),
  };
  state.ws.send(JSON.stringify(payload));
  addMessage({ sender_id: state.userId, content, msg_id: payload.temp_id }, true);
  dom.messageInput.value = "";
}

dom.registerTab.addEventListener("click", () => switchTab("register"));
dom.loginTab.addEventListener("click", () => switchTab("login"));

dom.registerForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    const body = {
      username: dom.regUsername.value.trim(),
      password: dom.regPassword.value,
      nickname: dom.regNickname.value.trim(),
    };
    const data = await request("/api/v1/auth/register", {
      method: "POST",
      body: JSON.stringify(body),
    });
    logLine(`Registered user_id=${data.user_id}.`);
    switchTab("login");
    dom.loginUsername.value = body.username;
    dom.loginPassword.value = body.password;
  } catch (err) {
    logLine(`Register failed: ${err.message}`);
  }
});

dom.loginForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    const data = await request("/api/v1/auth/login", {
      method: "POST",
      body: JSON.stringify({
        username: dom.loginUsername.value.trim(),
        password: dom.loginPassword.value,
      }),
    });
    state.token = data.token;
    state.userId = Number(data.user_id || 0);
    localStorage.setItem("nexus.token", state.token);
    localStorage.setItem("nexus.userId", String(state.userId));
    dom.tokenInput.value = state.token;
    renderAuthBadge();
    logLine("Login succeeded.");
  } catch (err) {
    logLine(`Login failed: ${err.message}`);
  }
});

dom.applyTokenBtn.addEventListener("click", () => {
  state.token = dom.tokenInput.value.trim();
  localStorage.setItem("nexus.token", state.token);
  renderAuthBadge();
  logLine("Token applied.");
});

dom.connectBtn.addEventListener("click", connectWs);
dom.historyBtn.addEventListener("click", loadHistory);
dom.sendForm.addEventListener("submit", sendMessage);

dom.aiHintBtn.addEventListener("click", () => {
  dom.messageInput.value = "@AI " + dom.messageInput.value.trim();
  dom.messageInput.focus();
});

dom.clearLogBtn.addEventListener("click", () => {
  dom.log.textContent = "";
});

window.addEventListener("beforeunload", () => {
  if (state.ws) {
    state.ws.close();
  }
});

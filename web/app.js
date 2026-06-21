(function () {
    "use strict";

    const tbody = document.getElementById("orders-body");
    const totalInfo = document.getElementById("total-info");
    const refreshBtn = document.getElementById("refresh-btn");
    const autoRefresh = document.getElementById("auto-refresh");
    const statusBar = document.getElementById("status-bar");
    const toastEl = createToast();

    const POLL_INTERVAL = 10000; // 自动刷新间隔 10s
    let timer = null;

    function init() {
        refreshBtn.addEventListener("click", fetchOrders);
        autoRefresh.addEventListener("change", toggleAutoRefresh);
        toggleAutoRefresh(); // 默认开启
        fetchOrders();
    }

    function toggleAutoRefresh() {
        if (timer) { clearInterval(timer); timer = null; }
        if (autoRefresh.checked) {
            timer = setInterval(fetchOrders, POLL_INTERVAL);
        }
    }

    async function fetchOrders() {
        try {
            const res = await fetch("/api/orders?page=1&page_size=50", { cache: "no-store" });
            if (!res.ok) throw new Error("HTTP " + res.status);
            const data = await res.json();
            render(data.orders || []);
            totalInfo.textContent = "共 " + (data.total ?? data.orders?.length ?? 0) + " 条";
            hideStatus();
        } catch (err) {
            showError("加载失败：" + err.message);
        }
    }

    function render(orders) {
        if (!orders.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="empty">暂无发货记录</td></tr>';
            return;
        }
        tbody.innerHTML = orders.map(o => `
            <tr>
                <td>${escapeHtml(String(o.id))}</td>
                <td class="email">${escapeHtml(o.buyer_email || "-")}</td>
                <td class="order-no">${escapeHtml(o.order_no || "-")}</td>
                <td class="cell-link">${linkCell(o.download_link)}</td>
                <td>${codeCell(o.extraction_code)}</td>
                <td>${statusBadge(o.status, o.error_message)}</td>
                <td class="time">${formatTime(o.created_at)}</td>
                <td class="time">${formatTime(o.notified_at)}</td>
            </tr>
        `).join("");

        // 绑定复制按钮
        tbody.querySelectorAll(".copy-btn").forEach(btn => {
            btn.addEventListener("click", () => copyText(btn.dataset.value));
        });
    }

    function statusBadge(status, errMsg) {
        const map = {
            notified: { cls: "notified", text: "已发货" },
            failed: { cls: "failed", text: "通知失败" },
            pending: { cls: "pending", text: "待通知" },
        };
        const s = map[status] || { cls: "pending", text: status || "未知" };
        const title = status === "failed" && errMsg ? ` title="${escapeAttr(errMsg)}"` : "";
        return `<span class="badge ${s.cls}"${title}>${s.text}</span>`;
    }

    function linkCell(url) {
        if (!url) return '<span class="muted-cell">-</span>';
        const safe = escapeHtml(url);
        return `<a href="${safe}" target="_blank" rel="noopener noreferrer">${safe}</a>`;
    }

    function codeCell(code) {
        if (!code) return '<span class="muted-cell">-</span>';
        const safe = escapeHtml(code);
        return `<span class="code">${safe}` +
               `<button class="copy-btn" data-value="${safe}" title="复制提取码">复制</button></span>`;
    }

    function formatTime(t) {
        if (!t) return "-";
        const d = new Date(t);
        if (isNaN(d.getTime())) return escapeHtml(String(t));
        const pad = n => String(n).padStart(2, "0");
        return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
               `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    }

    function showError(msg) {
        statusBar.textContent = msg;
        statusBar.className = "status-bar error";
    }
    function hideStatus() { statusBar.className = "status-bar hidden"; }

    function createToast() {
        const el = document.createElement("div");
        el.className = "toast";
        document.body.appendChild(el);
        return el;
    }
    function showToast(msg) {
        toastEl.textContent = msg;
        toastEl.classList.add("show");
        setTimeout(() => toastEl.classList.remove("show"), 1500);
    }

    async function copyText(text) {
        try {
            await navigator.clipboard.writeText(text);
            showToast("已复制：" + text);
        } catch {
            showToast("复制失败");
        }
    }

    function escapeHtml(s) {
        return String(s).replace(/[&<>"']/g, ch => ({
            "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
        }[ch]));
    }
    function escapeAttr(s) { return escapeHtml(s); }

    init();
})();

document.addEventListener('DOMContentLoaded', () => {
    const chatContainer = document.getElementById('chat-container');
    const chatForm = document.getElementById('chat-form');
    const userInput = document.getElementById('user-input');
    const sendBtn = document.getElementById('send-btn');
    const refreshBtn = document.getElementById('refresh-btn');
    const weatherBtn = document.getElementById('weather-btn');

    // Bot SVG Icon
    const BOT_ICON = `\u003csvg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"\u003e\u003cpath d="M12 2a2 2 0 0 1 2 2v2a2 2 0 0 1-2 2 2 2 0 0 1-2-2V4a2 2 0 0 1 2-2Z"/\u003e\u003cpath d="m8 22-1-11.2c-.1-1.1.6-2 1.6-2.2l5-1c.9-.2 1.8.5 1.9 1.4l.7 8"/\u003e\u003cpath d="M8 11h8"/\u003e\u003cpath d="M8 22h8"/\u003e\u003c/svg\u003e`;

    // User SVG Icon
    const USER_ICON = `\u003csvg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"\u003e\u003cpath d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/\u003e\u003ccircle cx="12" cy="7" r="4"/\u003e\u003c/svg\u003e`;

    function appendMessage(content, isUser = false, sources = []) {
        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${isUser ? 'user-message' : 'bot-message'}`;

        // Avatar
        const avatarDiv = document.createElement('div');
        avatarDiv.className = 'avatar';
        avatarDiv.innerHTML = isUser ? USER_ICON : BOT_ICON;

        // Content
        const contentDiv = document.createElement('div');
        contentDiv.className = 'message-content';

        // Simple markdown
        const formattedContent = content.replace(/\\*\\*(.*?)\\*\\*/g, '\u003cstrong\u003e$1\u003c/strong\u003e').replace(/\\n/g, '\u003cbr\u003e');
        contentDiv.innerHTML = formattedContent;

        messageDiv.appendChild(avatarDiv);
        messageDiv.appendChild(contentDiv);

        chatContainer.appendChild(messageDiv);

        // Add sources box if sources provided
        if (!isUser && sources && sources.length > 0) {
            const sourcesDiv = document.createElement('div');
            sourcesDiv.className = 'sources-box';
            sourcesDiv.innerHTML = `
                \u003cdiv class="sources-header"\u003e📄 Fuentes consultadas:\u003c/div\u003e
                \u003cdiv class="sources-list"\u003e
                    ${sources.map(src => `\u003cspan class="source-item"\u003e${src}\u003c/span\u003e`).join('')}
                \u003c/div\u003e
            `;
            chatContainer.appendChild(sourcesDiv);
        }

        chatContainer.scrollTop = chatContainer.scrollHeight;
    }

    function showTypingIndicator() {
        const indicatorDiv = document.createElement('div');
        indicatorDiv.className = 'message bot-message typing-indicator-container';
        indicatorDiv.id = 'typing-indicator';

        const avatarDiv = document.createElement('div');
        avatarDiv.className = 'avatar';
        avatarDiv.innerHTML = BOT_ICON;

        const contentDiv = document.createElement('div');
        contentDiv.className = 'message-content';
        contentDiv.innerHTML = `
            \u003cdiv class="typing-indicator"\u003e
                \u003cdiv class="dot"\u003e\u003c/div\u003e
                \u003cdiv class="dot"\u003e\u003c/div\u003e
                \u003cdiv class="dot"\u003e\u003c/div\u003e
            \u003c/div\u003e
        `;

        indicatorDiv.appendChild(avatarDiv);
        indicatorDiv.appendChild(contentDiv);

        chatContainer.appendChild(indicatorDiv);
        chatContainer.scrollTop = chatContainer.scrollHeight;
    }

    function removeTypingIndicator() {
        const indicator = document.getElementById('typing-indicator');
        if (indicator) {
            indicator.remove();
        }
    }

    async function sendMessage(message) {
        if (!message.trim()) return;

        appendMessage(message, true);
        userInput.value = '';
        sendBtn.disabled = true;
        showTypingIndicator();

        try {
            const response = await fetch('/api/ask', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ query: message }),
            });

            const data = await response.json();
            removeTypingIndicator();

            if (data.answer) {
                appendMessage(data.answer, false, data.sources || []);
            } else if (data.error) {
                appendMessage(`Error: ${data.error}`);
            } else {
                appendMessage("Something went wrong.");
            }
        } catch (error) {
            removeTypingIndicator();
            appendMessage(`Network Error: ${error.message}`);
        } finally {
            sendBtn.disabled = false;
            userInput.focus();
        }
    }

    async function refreshContext() {
        const icon = refreshBtn.querySelector('svg');
        icon.style.animation = 'spin 1s linear infinite';
        refreshBtn.disabled = true;

        // Use a temp bot message for feedback
        const feedbackId = Date.now();
        const messageDiv = document.createElement('div');
        messageDiv.className = 'message bot-message';
        messageDiv.innerHTML = `
            \u003cdiv class="avatar"\u003e${BOT_ICON}\u003c/div\u003e
            \u003cdiv class="message-content" id="fb-${feedbackId}"\u003eReloading drive files...\u003c/div\u003e
        `;
        chatContainer.appendChild(messageDiv);

        try {
            const response = await fetch('/api/refresh', { method: 'POST' });
            const data = await response.json();
            document.getElementById(`fb-${feedbackId}`).innerText = data.message;
        } catch (error) {
            document.getElementById(`fb-${feedbackId}`).innerText = "Failed to refresh context.";
        } finally {
            refreshBtn.disabled = false;
            icon.style.animation = 'none';
        }
    }

    chatForm.addEventListener('submit', (e) => {
        e.preventDefault();
        sendMessage(userInput.value);
    });

    refreshBtn.addEventListener('click', refreshContext);

    weatherBtn.addEventListener('click', () => {
        sendMessage("Consulta el clima actual en Oliveros, Santa Fe y dime la temperatura y estado del cielo.");
    });

    // Add CSS for spin animation dynamically
    const style = document.createElement('style');
    style.innerHTML = `
        @keyframes spin { 100% { transform: rotate(360deg); } }
    `;
    document.head.appendChild(style);
});

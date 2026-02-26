// Read the nextDay param from the URL and personalise the message
const params = new URLSearchParams(location.search);
const nextDay = params.get('nextDay') || 'tomorrow';
document.getElementById('msg').textContent =
    nextDay === 'Tuesday'
        ? "Next business day is Tuesday — print those appointment cards!"
        : "Print tomorrow's appointment cards before you forget!";

document.getElementById('dismiss').addEventListener('click', () => window.close());

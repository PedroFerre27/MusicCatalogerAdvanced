"""
Password policy enforcement (v0.1.9).

Verifica che le password dell'utente rispettino requisiti minimi di forza
PRIMA dell'hash. Senza dipendenze esterne (zxcvbn, haveibeenpwned, ecc.).
Per pilot 2 si può aggiungere un check k-anonymity contro HIBP.

Regole attuali (allineate alle linee guida NIST SP 800-63B 2024):
- Minimo 8 caratteri (limite tecnico già imposto da Pydantic)
- Massimo 128 caratteri (evita DoS bcrypt)
- Non in lista weak passwords comuni
- Non identica a email/username dell'utente
- Non deve essere una sequenza ripetitiva (aaaaaaaa, 12345678, abcdefgh)

NIST raccomanda di NON imporre regole di "complessità" tipo
"deve avere 1 maiuscola + 1 numero + 1 simbolo": studi mostrano che
forzano l'utente a creare pattern prevedibili (P@ssw0rd!) senza
aumentare la sicurezza reale. Meglio un check su lista weak + lunghezza.
"""
from typing import Optional


# Top-50 password più comuni (estratto da rockyou.txt + hibp).
# Lista compatta per non gonfiare l'image Docker.
_COMMON_WEAK = frozenset({
    "password", "12345678", "123456789", "qwerty123", "password1",
    "password123", "12345678a", "123abc123", "admin123", "letmein123",
    "welcome1", "welcome12", "welcome123", "iloveyou1", "iloveyou12",
    "qwertyuiop", "1q2w3e4r5t", "asdfghjkl", "zxcvbnmasd", "1234qwer",
    "qwer1234", "passw0rd", "p@ssw0rd", "admin1234", "rootroot",
    "toor1234", "changeme", "changeme1", "changeme123", "default1",
    "secret1", "secret123", "master1", "trustno1", "abcdefgh",
    "11111111", "00000000", "qazwsxedc", "asdfasdf", "qwertyqw",
    "letmeinplease", "starwars1", "football1", "baseball1", "monkey123",
    "dragon123", "shadow123", "freedom1", "whatever1", "sunshine1",
})


def _is_repetitive(pwd: str) -> bool:
    """True se la password è una sequenza ripetitiva ovvia.

    Esempi: "aaaaaaaa" (stesso carattere), "12345678" / "87654321"
    (numeri progressivi), "abcdefgh" (lettere progressive).
    """
    if len(set(pwd)) <= 2:
        return True
    # sequenza ascendente o discendente di codepoint consecutivi
    seq_asc = all(ord(pwd[i+1]) - ord(pwd[i]) == 1 for i in range(len(pwd)-1))
    seq_dsc = all(ord(pwd[i+1]) - ord(pwd[i]) == -1 for i in range(len(pwd)-1))
    return seq_asc or seq_dsc


def validate_password(
    password: str,
    *,
    email: Optional[str] = None,
    username: Optional[str] = None,
) -> Optional[str]:
    """
    Valida una password. Ritorna None se OK, altrimenti una stringa
    italiana con il motivo del rifiuto (mostrabile direttamente al
    user nel client).

    `email` e `username` opzionali per check anti-prevedibilità
    (la password non deve coincidere con uno dei due).
    """
    if password is None:
        return "La password non può essere vuota."

    n = len(password)
    if n < 8:
        return "La password deve avere almeno 8 caratteri."
    if n > 128:
        return "La password non può superare i 128 caratteri."

    pwd_lower = password.lower()

    # Lista comuni
    if pwd_lower in _COMMON_WEAK:
        return ("Questa password è troppo comune e facile da indovinare. "
                "Scegline una più originale.")

    # Coincide con email o username (full match o senza dominio)
    if email:
        e_lower = email.lower().strip()
        e_local = e_lower.split("@", 1)[0]
        if pwd_lower == e_lower or pwd_lower == e_local:
            return "La password non può essere uguale all'email."
    if username:
        u_lower = username.lower().strip()
        if pwd_lower == u_lower:
            return "La password non può essere uguale al nome utente."

    # Sequenze banali
    if _is_repetitive(password):
        return ("La password è una sequenza troppo ovvia "
                "(es. 12345678, abcdefgh, aaaaaaaa). Scegline un'altra.")

    return None  # OK

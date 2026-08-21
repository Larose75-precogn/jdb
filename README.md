# Journal de Banque (JdB)

Service auxiliaire de Structory : les transactions bancaires passent par une file de
propositions, sont validées par un utilisateur habilité, puis injectées au `journal.ledger`
de l'organisation. Le compte bancaire devient le flux maître de la comptabilité de trésorerie.

- Code du service : [`service/app.py`](service/app.py) (Flask, port 8086).
- Flux : `pull` (transactions via l'Executor) → sanctuarisation → `propositions` → `valider`/`rejeter` → import au journal.
- Fait partie de la plateforme Structory — voir https://github.com/Larose75-precogn/structory

Licence : Apache-2.0.

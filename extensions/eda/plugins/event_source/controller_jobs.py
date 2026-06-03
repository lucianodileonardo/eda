"""controller_jobs.py — EDA event source plugin.

Esegue polling sull'endpoint /api/v2/workflow_jobs/ dell'Automation Controller
e produce un evento per ogni workflow job concluso (successful o failed) di un
template tra quelli osservati. Mantiene un set di ID gia' visti per evitare il
ri-trigger dello stesso job ad ogni ciclo di polling (problema strutturale del
polling segnalato in fase di design).

Argomenti (passati dalla rulebook sotto `args`):
  controller_host : str   - es. "https://controller.example.com"
  token           : str   - OAuth token del Controller
  watch_templates : list  - nomi dei workflow job template da osservare
  interval        : int   - secondi tra un polling e l'altro (default 15)
  verify_ssl      : bool  - verifica TLS (default True)
  statuses        : list  - status da intercettare (default ["successful", "failed"])

Evento emesso:
  {
    "controller_job": {
      "id": 1234,
      "name": "WF-1",
      "status": "successful",
      "finished": "2026-06-03T10:00:00Z",
      "session_id": "abc-123",       # estratto dagli extra_vars del job
      "extra_vars": { ... }
    }
  }
"""

import asyncio
import json
from typing import Any, Dict, List, Set

import aiohttp


async def main(queue: asyncio.Queue, args: Dict[str, Any]) -> None:
    controller_host: str = args["controller_host"].rstrip("/")
    token: str = args["token"]
    watch_templates: List[str] = args.get("watch_templates", [])
    interval: int = int(args.get("interval", 15))
    verify_ssl: bool = bool(args.get("verify_ssl", True))
    statuses: List[str] = args.get("statuses", ["successful", "failed"])

    seen_ids: Set[int] = set()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{controller_host}/api/v2/workflow_jobs/"

    connector = aiohttp.TCPConnector(ssl=verify_ssl)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        while True:
            try:
                params = {
                    "order_by": "-finished",
                    "page_size": 20,
                }
                async with session.get(url, params=params) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

                for job in data.get("results", []):
                    job_id = job.get("id")
                    name = job.get("name")
                    status = job.get("status")

                    if job_id in seen_ids:
                        continue
                    if status not in statuses:
                        continue
                    if watch_templates and name not in watch_templates:
                        continue
                    # ignora job ancora in corso (finished == None)
                    if not job.get("finished"):
                        continue

                    seen_ids.add(job_id)
                    extra_vars = _parse_extra_vars(job.get("extra_vars"))

                    await queue.put(
                        {
                            "controller_job": {
                                "id": job_id,
                                "name": name,
                                "status": status,
                                "finished": job.get("finished"),
                                "session_id": extra_vars.get("session_id"),
                                "extra_vars": extra_vars,
                            }
                        }
                    )
            except Exception as exc:  # noqa: BLE001 - log e continua il polling
                await queue.put({"controller_poll_error": {"error": str(exc)}})

            await asyncio.sleep(interval)


def _parse_extra_vars(raw: Any) -> Dict[str, Any]:
    """extra_vars dal Controller arriva come stringa JSON (a volte gia' dict)."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return {}


if __name__ == "__main__":
    # Esecuzione standalone per debug rapido: stampa gli eventi su stdout.
    class _MockQueue:
        async def put(self, event: Dict[str, Any]) -> None:
            print(json.dumps(event))

    import sys

    mock_args = {
        "controller_host": sys.argv[1] if len(sys.argv) > 1 else "https://localhost",
        "token": sys.argv[2] if len(sys.argv) > 2 else "TOKEN",
        "watch_templates": ["WF-1", "WF-2", "WF-3"],
        "interval": 10,
        "verify_ssl": False,
    }
    asyncio.run(main(_MockQueue(), mock_args))

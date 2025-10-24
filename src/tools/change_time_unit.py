import re

def change_time_unit(timeframe: str) -> str | ValueError:
        if not isinstance(timeframe, str):
            raise ValueError("L'entrée doit être une chaîne de caractères (ex. '60s', '1m30s').")

        timeframe = timeframe.strip().lower()

        pattern = r'^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$'
        match = re.match(pattern, timeframe)
        if not match:
            raise ValueError("Format invalide. Utilisez un format comme '30s', '5m', '2h', ou '1m30s'.")

        # Extraire les valeurs (heures, minutes, secondes)
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)

        # Convertir tout en secondes pour validation
        total_seconds = hours * 3600 + minutes * 60 + seconds

        # Vérifier les limites (1 seconde à 1 jour = 86 400 secondes)
        if total_seconds < 1:
            raise ValueError("La durée doit être d'au moins 1 seconde.")
        if total_seconds > 86400:
            return "1 day"

        # Vérifier si total_seconds est un diviseur de 86 400
        if 86400 % total_seconds != 0:
            valid_intervals = [
                "1s", "2s", "3s", "4s", "5s", "6s", "8s", "10s", "12s", "15s", "16s",
                "20s", "24s", "30s", "32s", "36s", "40s", "48s", "1m", "2m", "3m", "4m",
                "5m", "6m", "8m", "10m", "12m", "15m", "16m", "20m", "24m", "30m", "1h",
                "2h", "3h", "4h", "6h", "8h", "12h", "24h"
            ]
            raise ValueError(
                f"L'intervalle {timeframe} ({total_seconds} secondes) n'est pas un diviseur de 24 heures (86 400 secondes). "
                f"Utilisez l'un des intervalles suivants : {', '.join(valid_intervals)}."
            )

        # Conversion en format lisible
        if total_seconds >= 3600:
            # Convertir en heures, minutes, secondes
            h = total_seconds // 3600
            remainder = total_seconds % 3600
            m = remainder // 60
            s = remainder % 60
            result = []
            if h > 0:
                result.append(f"{h} hour{'s' if h > 1 else ''}")
            if m > 0:
                result.append(f"{m} minute{'s' if m > 1 else ''}")
            if s > 0:
                result.append(f"{s} second{'s' if s > 1 else ''}")
            return " ".join(result) if result else "0 seconds"
        elif total_seconds >= 60:
            m = total_seconds // 60
            s = total_seconds % 60
            result = []
            if m > 0:
                result.append(f"{m} minute{'s' if m > 1 else ''}")
            if s > 0:
                result.append(f"{s} second{'s' if s > 1 else ''}")
            return " ".join(result) if result else "0 seconds"
        else:
            return f"{total_seconds} second{'s' if total_seconds > 1 else ''}"

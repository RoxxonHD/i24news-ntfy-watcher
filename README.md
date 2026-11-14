# i24NEWS News Feed Push Watcher (`ntfy.sh` + iOS Universal Links)

Servicio asíncrono en Python que monitorea cada 40 segundos el flujo en vivo **News Feed** de **i24NEWS** (`https://api.i24news.tv/v2/en/news`), filtra internamente las 10 secciones permitidas y envía notificaciones push minimalistas a tu iPhone vía **ntfy.sh**:
- **Solo el titular largo del News Feed** (cuerpo vacío, sin `excerpt`).
- **Sin emojis** (sin header `Tags`) y **sin prefijos entre corchetes**.
- **Con el logo oficial cuadrado de i24NEWS** (`https://www.i24news.tv/images/apple-touch-icon.png`).
- **Universal Link** directo hacia la app nativa de i24NEWS al tocar la notificación.

---

## Cómo desplegarlo GRATIS en la nube 24/7 (Paso a Paso para Principiantes)

La forma más sencilla y **100% gratuita** de dejarlo corriendo 24/7 con tu PC apagada es usando **GitHub + Render.com (Plan Free)** + **Cron-job.org** (para evitar que el servidor gratuito entre en reposo).

> **¿Qué pasa si el servidor en la nube se reinicia?**  
> `watcher.py` está diseñado con **arranque limpio automático**: cada vez que el contenedor arranca con la base de datos vacía, guarda silenciosamente las 25 noticias actuales del feed **sin enviar ninguna notificación** y a partir de los 40 segundos siguientes notifica únicamente las noticias nuevas.

### Paso 1: Subir estos archivos a tu cuenta de GitHub
1. Entra en [github.com](https://github.com/) e inicia sesión (o crea una cuenta gratuita).
2. Haz clic en el botón **+** (arriba a la derecha) -> **New repository**.
3. Ponle un nombre (por ejemplo `i24news-watcher`), selecciona **Private** (Privado) y haz clic en **Create repository**.
4. Haz clic en el enlace **uploading an existing file** que aparece en pantalla.
5. Arrastra desde tu carpeta `C:\Users\Equipo\.gemini\antigravity\scratch\i24news-ntfy-watcher` los archivos:
   - `watcher.py`
   - `requirements.txt`
   - `render.yaml`
   - `Dockerfile`
6. Haz clic en el botón verde **Commit changes**.

### Paso 2: Desplegar gratis en Render.com
1. Entra en [render.com](https://render.com/) y regístrate gratis usando el botón **Continue with GitHub**.
2. En el panel principal de Render, haz clic en **New +** (arriba a la derecha):
   - **Si tienes tarjeta/plan con Background Worker:** elige **Background Worker**, selecciona tu repositorio `i24news-watcher` y en **Start Command** escribe `python watcher.py`.
   - **Si quieres hacerlo 100% GRATIS sin costo mensual:** elige **Web Service** -> conecta tu repositorio `i24news-watcher`:
     - **Runtime:** `Python 3`
     - **Build Command:** `pip install -r requirements.txt`
     - **Start Command:** `python watcher.py`
     - **Instance Type:** selecciona **Free ($0/month)**.
     - Haz clic en **Create Web Service**.
3. ¡Listo! `watcher.py` detecta automáticamente el puerto de Render (`PORT`) y levanta un micro-servidor interno de salud (`/health`) mientras ejecuta el monitor de i24NEWS cada 40 segundos.

### Paso 3: Mantener el plan gratuito despierto 24/7 (Solo si elegiste Web Service Free)
Render apaga los *Web Services* gratuitos tras 15 minutos sin visitas. Para mantenerlo despierto las 24 horas gratis:
1. Copia la URL que te dio Render (por ejemplo: `https://i24news-ntfy-watcher.onrender.com`).
2. Entra en [cron-job.org](https://cron-job.org/) (o [uptimerobot.com](https://uptimerobot.com/)), crea una cuenta gratuita y pulsa **Create Cronjob**.
3. En **URL**, pega tu dirección de Render terminada en `/health` (ej. `https://i24news-ntfy-watcher.onrender.com/health`).
4. Configura el intervalo en **Every 5 minutes** (cada 5 minutos) y guarda.
5. Con eso tu servicio en Render nunca se dormirá y correrá 24/7 aunque apagues tu PC.

# Fuentes RSS seleccionadas

Se seleccionaron tres secciones del directorio RSS de El Tiempo. La página
del directorio no es un feed: de ella se tomaron las URL de los feeds.

| source_id | Sección | URL del feed | HTTP | Noticias en el feed |
|---|---|---|---|---|
| eltiempo_justicia | Justicia | https://www.eltiempo.com/rss/justicia.xml | 200 | 10 |
| eltiempo_arte_teatro | Arte y Teatro | https://www.eltiempo.com/rss/cultura_arte-y-teatro.xml | 200 | 10 |
| eltiempo_politica | Política | https://www.eltiempo.com/rss/politica.xml | 200 | 10 |

Verificadas el 2026-10-22.

## Campos disponibles por noticia (`<item>`)

Las 10 noticias de cada feed traen: `guid`, `title`, `link`, `description` y `pubDate`.

## Configuración

Las fuentes se definen en `config/sources.yaml` (medio, sección y URL).
Para cambiar una fuente se edita ese archivo, sin tocar el código.

## Observaciones

- Cada feed contiene solo las 10 noticias más recientes, por lo que el
  pipeline debe ejecutarse con frecuencia para no perder noticias.
- Justicia y Política pueden compartir noticias; se tratará en la capa Silver.

## Evidencias

Capturas en `docs/evidencias/fase1/`.

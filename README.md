# BigDataSnowflake

Лабораторная работа №1 по курсу «Анализ больших данных».  
Проект посвящён загрузке исходных CSV-данных в PostgreSQL и преобразованию их в аналитическую модель данных типа Snowflake.

## Как запустить

```bash
git clone https://github.com/cupydox/BDSnowflake.git
cd BDSnowflake
docker compose up -d

```
**Параметры подключения**

- **Host:** `127.0.0.1:55432`
- **Database:** `bdsnowflake`
- **User:** `ivan`
- **Password:** `password`

После запуска контейнера база поднимается автоматически, выполняются SQL-скрипты и загружаются исходные данные.
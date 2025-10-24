# Kelp

A small Node.js CSV processor that reads `data/users.csv`, transforms records (parses nested JSON fields), writes `output.json`, prints an age-distribution report to the console, and can insert processed rows into a database.


## Quick start (PowerShell)

1. Install dependencies:

```powershell
npm install
```

2. Create or update `.env` with your DB connection if you plan to insert into Postgres.

3. Start the server:

```powershell
node server.js
# Server listens on port 3000 by default
```

4. Trigger processing (example): send a POST request to `/process` with JSON body pointing to the CSV path. 

You can use `curl`, Postman, or any HTTP client to POST to `http://localhost:3000/process`.

## Sample output
Below are the example screenshots from this project's outputs:

- API request/response (Postman):

![API Request/Response](readmeImage\Postman.png)

- Database (psql) view of inserted rows (address and additional_info JSON columns):

![Database output](readmeImage\Postgres.png)

- Console run showing the age distribution report and processing summary:

![Console output](readmeImage\image.png)

If the screenshots are not present, add them to `docs/screenshots/` with the filenames above to show them inline in this README.


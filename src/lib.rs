use libsql::wasm::{CloudflareSender, Connection};
use libsql::{params, Rows, Value};
use serde_json::json;
use simple_base64::prelude::BASE64_STANDARD_NO_PAD;
use simple_base64::Engine;
use std::collections::HashMap;
use worker::*;

mod utils;

// Log each request to dev console
fn log_request(req: &Request) {
    tracing::info!(
        "[{}], located at: {:?}, within: {}",
        req.path(),
        req.cf().coordinates().unwrap_or_default(),
        req.cf().region().unwrap_or_else(|| "unknown region".into())
    );
}

// Take a query result and render it into a HTML table
fn result_to_html_table(mut result: Rows) -> String {
    let mut html = "<table style=\"border: 1px solid\">".to_string();
    let col_num = result.column_count();
    for col in 0..col_num {
        let column = result.column_name(col).unwrap_or("");
        html += &format!("<th style=\"border: 1px solid\">{column}</th>");
    }
    while let Some(row) = result.next().unwrap() {
        html += "<tr style=\"border: 1px solid\">";
        for col in 0..col_num {
            let cell = row.get_value(col).unwrap();
            html += &format!("<td>{}</td>", stringify(&cell));
        }
        html += "</tr>";
    }
    html += "</table>";
    html
}

fn stringify(cell: &Value) -> String {
    match cell {
        Value::Null => "".to_string(),
        Value::Integer(v) => v.to_string(),
        Value::Real(v) => v.to_string(),
        Value::Text(v) => v.clone(),
        Value::Blob(v) => BASE64_STANDARD_NO_PAD.encode(&v),
    }
}

// Create a javascript canvas which loads a map of visited airports
fn create_map_canvas(mut result: Rows) -> String {
    let mut canvas = r#"
  <script src="https://cdnjs.cloudflare.com/ajax/libs/p5.js/0.5.16/p5.min.js" type="text/javascript"></script>
  <script src="https://unpkg.com/mappa-mundi/dist/mappa.js" type="text/javascript"></script>
    <script>
    let myMap;
    let canvas;
    const mappa = new Mappa('Leaflet');
    const options = {
      lat: 0,
      lng: 0,
      zoom: 2,
      style: "http://{s}.tile.osm.org/{z}/{x}/{y}.png"
    }

    function setup(){
      canvas = createCanvas(640,480);
      myMap = mappa.tileMap(options); 
      myMap.overlay(canvas) 
    
      fill(200, 100, 100);
      myMap.onChange(drawPoint);
    }

    function draw(){
    }

    function drawPoint(){
      clear();
      let point;"#.to_owned();

    while let Some(row) = result.next().unwrap() {
        let airport: String = row.get(0).unwrap();
        let lat: f64 = row.get(1).unwrap();
        let lon: f64 = row.get(2).unwrap();
        canvas += &format!(
            "point = myMap.latLngToPixel({}, {});\nellipse(point.x, point.y, 10, 10);\ntext({}, point.x, point.y);\n",
            // NOTICE: value_map is not very efficient and only enabled if the feature "mapping_names_to_values_in_rows" is enabled
            lat, lon, airport
        );
    }
    canvas += "}</script>";
    canvas
}

// Serve a request to load the page
async fn serve(
    airport: impl Into<String>,
    country: impl Into<String>,
    city: impl Into<String>,
    coordinates: (f32, f32),
    db: &Connection<CloudflareSender>,
) -> anyhow::Result<String> {
    let airport = airport.into();
    let country = country.into();
    let city = city.into();

    // Recreate the tables if they do not exist yet

    if let Err(e) = db.execute_batch(r#"
    BEGIN;
        CREATE TABLE IF NOT EXISTS counter(country TEXT, city TEXT, value, PRIMARY KEY(country, city)) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS coordinates(lat INT, long INT, airport TEXT, PRIMARY KEY (lat, long));
    END;
    "#).await {
        tracing::error!("Error creating table: {e}");
        anyhow::bail!("{e}")
    }
    db.execute(
        "INSERT OR IGNORE INTO counter VALUES (?, ?, 0)",
        params![country.clone(), city.clone()],
    )
    .await?;
    db.execute(
        "UPDATE counter SET value = value + 1 WHERE country = ? AND city = ?",
        params![country, city],
    )
    .await?;
    db.execute(
        "INSERT OR IGNORE INTO coordinates VALUES (?, ?, ?)",
        // Parameters with different types can be passed to a convenience macro - args!()
        params![coordinates.0, coordinates.1, airport],
    )
    .await?;
    let counter_response = db.query("SELECT * FROM counter", ()).await?;
    let scoreboard = result_to_html_table(counter_response);

    let canvas = create_map_canvas(
        db.query("SELECT airport, lat, long FROM coordinates", ())
            .await?,
    );
    let html = format!(
        r#"
        <body>
        {canvas} Database powered by <a href="https://chiselstrike.com/">Turso</a>.
        <br /> Scoreboard: <br /> {scoreboard}
        <footer>Map data from OpenStreetMap (https://tile.osm.org/)</footer>
        </body>
        "#
    );
    Ok(html)
}

fn open_connection(env: &Env) -> anyhow::Result<Connection<CloudflareSender>> {
    let url = env
        .secret("LIBSQL_CLIENT_URL")
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .to_string();
    let token = env
        .secret("LIBSQL_CLIENT_TOKEN")
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .to_string();
    Ok(Connection::open_cloudflare_worker(url, token))
}

#[event(fetch)]
pub async fn main(req: Request, env: Env, _ctx: worker::Context) -> Result<Response> {
    log_request(&req);

    utils::set_panic_hook();
    let router = Router::new();

    tracing_worker::init(&env);

    router
        .get_async("/", |req, ctx| async move {
            let db = match open_connection(&ctx.env) {
                Ok(client) => client,
                Err(e) => return Response::error(e.to_string(), 500),
            };
            let cf = req.cf();
            let airport = cf.colo();
            let country = cf.country().unwrap_or_default();
            let city = cf.city().unwrap_or_default();
            let coordinates = cf.coordinates().unwrap_or_default();
            match serve(airport, country, city, coordinates, &db).await {
                Ok(html) => Response::from_html(html),
                Err(e) => Response::ok(format!("Error: {e}")),
            }
        })
        .get("/worker-version", |_, ctx| {
            let version = ctx.var("WORKERS_RS_VERSION")?.to_string();
            Response::ok(version)
        })
        .get("/locate", |req, _ctx| {
            let cf = req.cf();
            let airport = cf.colo();
            let country = cf.country().unwrap_or_default();
            let city = cf.city().unwrap_or_default();
            let coordinates = cf.coordinates().unwrap_or_default();
            Response::ok(format!(
                "{};{};{};{};{}",
                airport, country, city, coordinates.0, coordinates.1
            ))
        })
        .get_async("/users", |_, ctx| async move {
            let db = match open_connection(&ctx.env) {
                Ok(client) => client,
                Err(e) => return Response::error(e.to_string(), 500),
            };
            let stmt = "select * from example_users";
            let rows = match db.query(stmt, ()).await {
                Ok(rows) => rows,
                Err(e) => return Response::error(e.to_string(), 500),
            };
            let json = match into_json(rows) {
                Ok(json) => json,
                Err(e) => return Response::error(e.to_string(), 500),
            };
            Response::from_json(&json)
        })
        .get_async("/add-user", |req, ctx| async move {
            let url = req.url().unwrap();
            let hash_query: HashMap<String, String> = url.query_pairs().into_owned().collect();
            let email = match hash_query.get("email") {
                Some(string) => string,
                None => return Response::error("No email", 400),
            };

            let db = match open_connection(&ctx.env) {
                Ok(client) => client,
                Err(e) => return Response::error(e.to_string(), 500),
            };

            match db
                .execute(
                    "insert into example_users values (?)",
                    params![email.clone()],
                )
                .await
            {
                Ok(_) => Response::from_json(&serde_json::json!({
                    "result": "Added"
                })),
                Err(e) => Response::error(e.to_string(), 500),
            }
        })
        .run(req, env)
        .await
}

fn into_json(mut res: Rows) -> anyhow::Result<serde_json::Value> {
    let col_num = res.column_count();
    let cols: Vec<_> = (0..col_num)
        .map(|i| res.column_name(i).map(String::from))
        .collect();
    let mut rows = Vec::new();
    while let Some(row) = res.next()? {
        let r: Vec<_> = (0..col_num)
            .map(|i| match row.get_value(i).unwrap() {
                Value::Null => serde_json::Value::Null,
                Value::Integer(v) => serde_json::Value::from(v),
                Value::Real(v) => serde_json::Value::from(v),
                Value::Text(v) => serde_json::Value::from(v),
                Value::Blob(v) => {
                    let b = BASE64_STANDARD_NO_PAD.encode(&v);
                    json!({ "base64": b })
                }
            })
            .collect();
        rows.push(r);
    }

    Ok(json!({
        "columns": cols,
        "rows": rows
    }))
}

#[cfg(test)]
mod tests {
    use libsql::wasm::{CloudflareSender, Connection};

    fn test_db() -> Connection<CloudflareSender> {
        let url = env!("LIBSQL_CLIENT_URL");
        let auth_token = env!("LIBSQL_CLIENT_TOKEN");
        Connection::open_cloudflare_worker(url, auth_token)
    }

    #[tokio::test]
    async fn test_counter_updated() {
        let db = test_db();

        let payloads = [
            ("waw", "PL", "Warsaw", (52.1672, 20.9679)),
            ("waw", "PL", "Warsaw", (52.1672, 20.9679)),
            ("waw", "PL", "Warsaw", (52.1672, 20.9679)),
            ("hel", "FI", "Helsinki", (60.3183, 24.9497)),
            ("hel", "FI", "Helsinki", (60.3183, 24.9497)),
        ];

        for p in payloads {
            super::serve(p.0, p.1, p.2, p.3, &db).await.unwrap();
        }

        let mut result = db
            .query("SELECT country, city, value FROM counter", ())
            .await
            .unwrap();
        let columns: Vec<_> = (0..result.column_count())
            .map(|c| result.column_name(c).unwrap_or(""))
            .collect();

        assert_eq!(columns, vec!["country", "city", "value"]);
        while let Some(row) = result.next().unwrap() {
            let city: String = row.get(1).unwrap();
            match city.as_str() {
                "Warsaw" => assert_eq!(row.get::<i64>(2).unwrap(), 3),
                "Helsinki" => assert_eq!(row.get::<i64>(2).unwrap(), 2),
                other => panic!("Unknown city: {:?}", other),
            }
        }
    }
}

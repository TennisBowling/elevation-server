use std::net::SocketAddr;
use std::sync::Arc;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{routing, Router};
use axum_macros::debug_handler;
use gdal::Dataset;
use gdal::raster::RasterBand;
use tracing_subscriber::EnvFilter;


struct ElevationData {
    _dataset: Dataset, // Keep dataset alive
    rasterband: RasterBand<'static>,
}

// This is so jank. Should be ok since we're only reading from it
unsafe impl Send for ElevationData {}
unsafe impl Sync for ElevationData {}

#[inline(always)]
fn get_elev(band: &RasterBand, transform: &[f64; 6], lat: &f64, lon: &f64) -> f64 {
    let col = ((lon - transform[0]) / transform[1]) as isize;
    let row = ((lat - transform[3]) / transform[5]) as isize;

    let buf = match band.read_as::<f64>(
        (col, row),
        (1, 1),
        (1, 1),
        Some(gdal::raster::ResampleAlg::CubicSpline),
    ) {
        Ok(buf) => buf,
        Err(_) => {
            return f32::MIN as f64;
        }
    };
    buf.data()[0]
}

#[debug_handler(state = (Arc<ElevationData>, Arc<[f64; 6]>))]
async fn get_elev_route(
    State((elev_data, transform)): State<(Arc<ElevationData>, Arc<[f64; 6]>)>,
    body: String,
) -> impl IntoResponse {


    let mut latlon = body.split(',');
    let lat = latlon.next().unwrap().parse::<i32>().unwrap();
    let lon = latlon.next().unwrap().parse::<i32>().unwrap();

    let elev = get_elev(
        &elev_data.rasterband,
        &transform,
        &(lat as f64 * 1e-7),
        &(lon as f64 * 1e-7),
    ) as f32;

    if elev == f32::MIN {
        tracing::warn!("Couldn't find elevation");
    }

    (StatusCode::OK, elev.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_new("trace,hyper=info").unwrap();

    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

    let matches = clap::App::new("elevation-server")
        .version("0.0.1")
        .author("TennisBowling <tennisbowling@tennisbowling.com>")
        .setting(clap::AppSettings::ColoredHelp)
        .about("elevation server")
        .arg(
            clap::Arg::with_name("d")
                .long("elev-file")
                .help("location of elevation .tif file")
                .takes_value(true)
                .required(true)
        )
        .get_matches();

    let location = matches.value_of("d").unwrap();

    let dataset = Dataset::open(location).unwrap();
    tracing::info!("Opened elevation dataset");



    let elev_data = Arc::new(ElevationData {
        rasterband: unsafe { std::mem::transmute(dataset.rasterband(1).unwrap()) },     // We have to get the rasterband as 'static. We KNOW it'll exist for the lifetime of the server
        _dataset: dataset,
    });

    let elev_transform: Arc<[f64; 6]> = Arc::new(elev_data._dataset.geo_transform().expect("Failed to get geotransform"));

    let app = Router::new()
        .route("/", routing::post(get_elev_route))
        .with_state((elev_data, elev_transform));




    let addr = "0.0.0.0:8083".to_string();
    let addr: SocketAddr = addr.parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Listening on {}", addr);
    
    axum::serve(listener, app).await?;
    Ok(())
}
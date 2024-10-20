use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use futures_util::{SinkExt, StreamExt};
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

    let buf = band.read_as::<f64>(
        (col, row),
        (1, 1),
        (1, 1),
        Some(gdal::raster::ResampleAlg::CubicSpline),
    ).unwrap();
    buf.data()[0]
}

async fn handle_connection(
    stream: TcpStream,
    elev_data: Arc<ElevationData>,
    transform: Arc<[f64; 6]>,
) {
    let ws_stream = accept_async(stream).await.unwrap();
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    while let Some(msg) = ws_receiver.next().await {
        let msg = msg.unwrap();

        let mut latlon = msg.to_text().unwrap().split(',');
        let lat = latlon.next().unwrap().parse::<i32>().unwrap();
        let lon = latlon.next().unwrap().parse::<i32>().unwrap();


        let elev = get_elev(
            &elev_data.rasterband,
            &transform,
            &(lat as f64 * 1e-7),
            &(lon as f64 * 1e-7),
        ) as f32;

        if elev == f32::MIN {
            tracing::warn!("Elevation not found");
        }

        ws_sender.send(Message::Text(elev.to_string())).await.unwrap();
    }
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
        rasterband: unsafe { std::mem::transmute(dataset.rasterband(1).unwrap()) }, // We have to get the rasterband as 'static. We KNOW it'll exist for the lifetime of the server
        _dataset: dataset,
    });

    let elev_transform: Arc<[f64; 6]> = Arc::new(elev_data._dataset.geo_transform().unwrap());


    let addr = "0.0.0.0:8083".to_string();
    let addr: SocketAddr = addr.parse().unwrap();
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("Websocket server listening on {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        let elev_data = Arc::clone(&elev_data);
        let transform = Arc::clone(&elev_transform);

        tokio::spawn(async move {
            handle_connection(stream, elev_data, transform).await;
        });
    }

    Ok(())
}
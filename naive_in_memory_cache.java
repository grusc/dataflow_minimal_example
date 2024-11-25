public class GeolocationSingle extends DoFn<SwrAssetModel, SwrAssetModel> {
    private static final Cache<String, Location> CACHE = Caffeine.newBuilder().initialCapacity(500).build();
    private HttpClient client;

    @Setup
    public void connect() {
        this.client = HttpClient.newHttpClient();
    }

    @ProcessElement
    public void process(ProcessContext ctx) {
        SwrAssetModel element = ctx.element();
        if (element == null) {
            return;
        }

        var city = element.getString(SwrItemBqColumn.city);
        if (city == null) {
            ctx.output(element);
            return;
        }

        Location location = CACHE.getIfPresent(city);
        if (location == null) {
            location = getCityFromApi(city);
            CACHE.put(city, location);
        }
        if (location == null) {
            ctx.output(element);
            return;
        }

        SwrAssetModel output = (SwrAssetModel) element.clone();
        output.put(SwrItemBqColumn.locations, locations);

        ctx.output(output);
    }

}

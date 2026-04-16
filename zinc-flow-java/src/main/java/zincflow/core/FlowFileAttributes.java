package zincflow.core;

/// Conventional FlowFile attribute names. Not exhaustive — processors
/// are free to set anything — but every attribute that appears in more
/// than one built-in processor or test lives here so a rename ripples
/// in one place.
///
/// Grouped by producer for easier navigation.
public final class FlowFileAttributes {

    // --- Common (set by sources) -----------------------------------------
    /// Filename of the on-disk source, no path.
    public static final String FILENAME = "filename";
    /// Absolute path of the source file.
    public static final String PATH = "path";
    /// Raw byte size of the source payload.
    public static final String SIZE = "size";
    /// Identifier of the source that emitted the FlowFile.
    public static final String SOURCE = "source";

    // --- FlowFile V3 framing ---------------------------------------------
    /// Zero-based index of the frame within a V3 bundle.
    public static final String V3_FRAME_INDEX = "v3.frame.index";
    /// Total frame count in the V3 bundle.
    public static final String V3_FRAME_COUNT = "v3.frame.count";

    // --- PutHTTP outcomes -------------------------------------------------
    /// HTTP status code from the last PutHTTP call.
    public static final String PUTHTTP_STATUS = "puthttp.status";
    /// Byte size of the response body.
    public static final String PUTHTTP_RESPONSE_SIZE = "puthttp.response.size";

    // --- GenerateFlowFile -------------------------------------------------
    /// Monotonic counter emitted by GenerateFlowFile.
    public static final String GENERATE_INDEX = "generate.index";
    /// Optional content type set by GenerateFlowFile / HTTP ingress.
    public static final String HTTP_CONTENT_TYPE = "http.content.type";

    private FlowFileAttributes() {}
}

mod tracing;
mod tracing_grpc;

#[cfg(test)]
mod tests {
    use super::tracing_grpc;
    #[test]
    fn test_start_server() {
        tracing_grpc::create_sky_tracing()
    }
}
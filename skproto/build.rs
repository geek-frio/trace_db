use protobuf_build::Builder;

fn main() {
    Builder::new()
        .search_dir_for_protos("proto")
        .append_to_black_list("eraftpb")
        .generate()
}

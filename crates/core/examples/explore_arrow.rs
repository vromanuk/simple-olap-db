use arrow::array::{as_boolean_array, as_string_array, Array, AsArray};
use arrow::compute::{filter, sum};
use arrow::datatypes::{Int32Type, Int64Type};
use arrow::util::pretty::print_batches;
use olap_core::sample_data::create_sample_batch;

fn main() {
    let batch = create_sample_batch();

    println!("=== Pretty Print ===");
    print_batches(&[batch.clone()]).unwrap();

    println!("\n=== Manual Column Access ===");
    let page_urls = as_string_array(batch.column(1));
    let user_ids = batch.column(2).as_primitive::<Int64Type>();
    let durations = batch.column(4).as_primitive::<Int32Type>();
    let is_mobile = as_boolean_array(batch.column(6));

    for i in 0..batch.num_rows() {
        let page = page_urls.value(i);
        let user = if user_ids.is_null(i) {
            "anon".to_string()
        } else {
            user_ids.value(i).to_string()
        };
        let dur = if durations.is_null(i) {
            "n/a".to_string()
        } else {
            format!("{}ms", durations.value(i))
        };
        let mobile = if is_mobile.value(i) {
            "mobile"
        } else {
            "desktop"
        };

        println!("  [{i}] {page:<12} user={user:<6} duration={dur:<8} {mobile}");
    }

    println!("\n=== Avg Duration for Mobile Users ===");

    // Approach 1: row-at-a-time loop
    let mut total = 0i64;
    let mut count = 0i64;
    for i in 0..batch.num_rows() {
        if is_mobile.value(i) && !durations.is_null(i) {
            total += durations.value(i) as i64;
            count += 1;
        }
    }
    let avg_manual = total as f64 / count as f64;
    println!("  row-at-a-time:  avg = {avg_manual:.1}ms ({count} values)");

    // Approach 2: Arrow compute kernels
    let mobile_durations = filter(durations, is_mobile).unwrap();
    let mobile_durations = mobile_durations.as_primitive::<Int32Type>();
    let kernel_sum = sum::<Int32Type>(mobile_durations).unwrap_or(0);
    let kernel_count = mobile_durations.len() - mobile_durations.null_count();
    let avg_kernel = kernel_sum as f64 / kernel_count as f64;
    println!("  compute kernel: avg = {avg_kernel:.1}ms ({kernel_count} values)");
}

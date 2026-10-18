//! 本机键碰撞率实测:扫描全部数据源,报告逐源去重键碰撞计数。
//! 用法:cargo run -p usagebook-core --example collision_rate

fn main() {
    let mut total_clash = 0u64;
    let mut total_records = 0usize;
    for a in usagebook_core::registered_sources() {
        let (records, stats) = a.scan(None);
        if records.is_empty() && stats.key_clash == 0 {
            continue;
        }
        println!(
            "{:<10} 记录 {:>7} · 键碰撞 {}",
            a.id(),
            records.len(),
            stats.key_clash
        );
        total_clash += stats.key_clash;
        total_records += records.len();
    }
    println!("—— 合计:记录 {total_records} · 键碰撞 {total_clash}");
    if total_clash == 0 {
        println!("碰撞率 0:计数上报已就位,无需进一步处置。");
    } else {
        println!("存在键碰撞!请结合源档案评估去重键是否需要加固。");
    }
}

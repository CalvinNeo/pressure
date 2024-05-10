use std::io::BufRead;

pub fn read_lines_lazy(file_path: &str) -> impl Iterator<Item = std::io::Result<String>> {
    let file = std::fs::File::open(file_path).unwrap();
    let reader = std::io::BufReader::new(file);
    reader.lines().map(|line| {
        line.map(|l| {
            let v: Vec<String> = l.trim().split(' ').take(1).map(|e| e.to_string()).collect();
            v.into_iter().next().unwrap()
        })
    })
}

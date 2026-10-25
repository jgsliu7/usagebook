//! Small, source-independent graph guards. No protocol or billing inference lives here.
use std::collections::{HashMap, VecDeque};
use std::path::{Component, Path, PathBuf};

pub(super) fn lexical(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for part in path.components() {
        match part {
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            part => result.push(part.as_os_str()),
        }
    }
    result
}

pub(super) fn physical(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| lexical(path))
}

/// Retain one reader per physical file, but remember every discovered spelling
/// so a parentSession naming an authorized alias can still be resolved.
pub(super) fn unique_files(files: &mut Vec<PathBuf>) -> HashMap<PathBuf, Vec<usize>> {
    let mut identities = HashMap::new();
    let mut aliases: HashMap<PathBuf, Vec<usize>> = HashMap::new();
    let mut unique = Vec::new();
    for path in files.drain(..) {
        let canonical = physical(&path);
        let key = crate::common::file_stamp(&path)
            .map(|s| s.identity_key())
            .filter(|s| !s.is_empty())
            .map(|s| s.split('/').take(2).collect::<Vec<_>>().join("/"))
            .unwrap_or_else(|| canonical.to_string_lossy().into_owned());
        let index = *identities.entry(key).or_insert_with(|| {
            let index = unique.len();
            unique.push(path.clone());
            index
        });
        for alias in [lexical(&path), canonical] {
            let indexes = aliases.entry(alias).or_default();
            if !indexes.contains(&index) {
                indexes.push(index);
            }
        }
    }
    *files = unique;
    aliases
}

/// Parent cycles and a failed proof quarantine the whole connected lineage,
/// including its readable ancestors. Unrelated sessions remain usable.
pub(super) fn quarantine(parents: &[Vec<usize>], mut bad: Vec<bool>) -> Vec<bool> {
    let mut visited = vec![0; parents.len()];
    for start in 0..parents.len() {
        let mut chain = Vec::new();
        let mut current = start;
        while visited[current] == 0 && parents[current].len() == 1 {
            visited[current] = 1;
            chain.push(current);
            current = parents[current][0];
        }
        if visited[current] == 1 {
            bad[current] = true;
        }
        for index in chain {
            visited[index] = 2;
        }
    }
    let mut neighbors = vec![Vec::new(); parents.len()];
    for (child, candidates) in parents.iter().enumerate() {
        for &parent in candidates {
            neighbors[child].push(parent);
            neighbors[parent].push(child);
        }
    }
    let mut queue: VecDeque<_> = bad
        .iter()
        .enumerate()
        .filter_map(|(i, b)| b.then_some(i))
        .collect();
    while let Some(index) = queue.pop_front() {
        for &neighbor in &neighbors[index] {
            if !bad[neighbor] {
                bad[neighbor] = true;
                queue.push_back(neighbor);
            }
        }
    }
    bad
}

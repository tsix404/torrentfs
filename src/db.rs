     1|use rusqlite::{params, Connection, OptionalExtension};
     2|use std::path::Path;
     3|
     4|#[derive(Debug, thiserror::Error)]
     5|pub enum DbError {
     6|    #[error("database error: {0}")]
     7|    Sqlite(#[from] rusqlite::Error),
     8|    #[error("torrent with source_path already exists: {0}")]
     9|    SourcePathExists(String),
    10|    #[error("migration error: {0}")]
    11|    Migration(String),
    12|}
    13|
    14|#[derive(Debug, Clone, PartialEq)]
    15|pub enum TorrentStatus {
    16|    Pending,
    17|    Downloading,
    18|    Seeding,
    19|    Error,
    20|}
    21|
    22|impl TorrentStatus {
    23|    pub fn as_str(&self) -> &'static str {
    24|        match self {
    25|            TorrentStatus::Pending => "pending",
    26|            TorrentStatus::Downloading => "downloading",
    27|            TorrentStatus::Seeding => "seeding",
    28|            TorrentStatus::Error => "error",
    29|        }
    30|    }
    31|}
    32|
    33|impl From<&str> for TorrentStatus {
    34|    fn from(s: &str) -> Self {
    35|        match s {
    36|            "downloading" => TorrentStatus::Downloading,
    37|            "seeding" => TorrentStatus::Seeding,
    38|            "error" => TorrentStatus::Error,
    39|            _ => TorrentStatus::Pending,
    40|        }
    41|    }
    42|}
    43|
    44|impl From<String> for TorrentStatus {
    45|    fn from(s: String) -> Self {
    46|        TorrentStatus::from(s.as_str())
    47|    }
    48|}
    49|
    50|#[derive(Debug, Clone)]
    51|pub struct Torrent {
    52|    pub id: i64,
    53|    pub source_path: String,
    54|    pub name: String,
    55|    pub filename: String,
    56|    pub total_size: i64,
    57|    pub info_hash: String,
    58|    pub file_count: i64,
    59|    pub status: TorrentStatus,
    60|    pub torrent_data: Option<Vec<u8>>,
    61|    pub resume_data: Option<Vec<u8>>,
    62|    pub created_at: String,
    63|}
    64|
    65|#[derive(Debug, Clone)]
    66|pub struct TorrentFile {
    67|    pub id: i64,
    68|    pub torrent_id: i64,
    69|    pub directory_id: Option<i64>,
    70|    pub name: String,
    71|    pub path: String,
    72|    pub size: i64,
    73|    pub first_piece: i64,
    74|    pub last_piece: i64,
    75|    pub piece_start: Option<i64>,
    76|    pub piece_end: Option<i64>,
    77|}
    78|
    79|#[derive(Debug, Clone)]
    80|pub struct TorrentDirectory {
    81|    pub id: i64,
    82|    pub torrent_id: i64,
    83|    pub parent_id: Option<i64>,
    84|    pub name: String,
    85|}
    86|
    87|#[derive(Debug, Clone, PartialEq)]
    88|pub enum InsertTorrentResult {
    89|    Inserted(i64),
    90|    Duplicate(i64),
    91|}
    92|
    93|pub struct FileEntry {
    94|    pub path: String,
    95|    pub size: i64,
    96|}
    97|
    98|pub struct Database {
    99|    conn: Connection,
   100|}
   101|
   102|impl Database {
   103|    pub fn open(path: &Path) -> Result<Self, DbError> {
   104|        let conn = Connection::open(path)?;
   105|        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
   106|        let mut db = Self { conn };
   107|        db.run_migrations()?;
   108|        Ok(db)
   109|    }
   110|
   111|    pub fn open_in_memory() -> Result<Self, DbError> {
   112|        let conn = Connection::open_in_memory()?;
   113|        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
   114|        let mut db = Self { conn };
   115|        db.run_migrations()?;
   116|        Ok(db)
   117|    }
   118|
   119|    fn run_migrations(&mut self) -> Result<(), DbError> {
   120|        let tx = self.conn.transaction()?;
   121|        let user_version: i64 = tx
   122|            .pragma_query_value(None, "user_version", |row| row.get(0))
   123|            .optional()?
   124|            .unwrap_or(0);
   125|
   126|        if user_version < 1 {
   127|            Self::migrate_v1(&tx)?;
   128|            tx.pragma_update(None, "user_version", 2)?;
   129|        } else if user_version == 1 {
   130|            Self::migrate_v2(&tx)?;
   131|            tx.pragma_update(None, "user_version", 2)?;
   132|        }
   133|        
   134|        if user_version < 3 {
   135|            Self::migrate_v3(&tx)?;
   136|            tx.pragma_update(None, "user_version", 3)?;
   137|        }
   138|
   139|        if user_version < 4 {
   140|            Self::migrate_v4(&tx)?;
   141|            tx.pragma_update(None, "user_version", 4)?;
   142|        }
   143|
   144|        tx.commit()?;
   145|        
   146|        if user_version < 3 {
   147|            let paths: Vec<String> = {
   148|                let mut stmt = self.conn.prepare(
   149|                    "SELECT DISTINCT source_path FROM torrents WHERE source_path != ''",
   150|                )?;
   151|                let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
   152|                rows.collect::<Result<Vec<_>, _>>()?
   153|            };
   154|
   155|            for path in paths {
   156|                if let Err(e) = self.ensure_metadata_directories(&path) {
   157|                    tracing::warn!("Failed to create metadata directories for {}: {}", path, e);
   158|                }
   159|            }
   160|        }
   161|
   162|        Ok(())
   163|    }
   164|
   165|    fn migrate_v1(conn: &Connection) -> Result<(), DbError> {
   166|        conn.execute_batch(
   167|            "CREATE TABLE IF NOT EXISTS torrents (
   168|                id INTEGER PRIMARY KEY AUTOINCREMENT,
   169|                info_hash TEXT NOT NULL,
   170|                name TEXT NOT NULL,
   171|                total_size INTEGER NOT NULL,
   172|                file_count INTEGER NOT NULL DEFAULT 1,
   173|                status TEXT NOT NULL DEFAULT 'pending',
   174|                source_path TEXT NOT NULL DEFAULT '',
   175|                torrent_data BLOB,
   176|                resume_data BLOB,
   177|                created_at DATETIME NOT NULL DEFAULT (datetime('now')),
   178|                UNIQUE(info_hash, source_path)
   179|            );
   180|
   181|            CREATE INDEX IF NOT EXISTS idx_torrents_info_hash ON torrents(info_hash);
   182|            CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status);
   183|            CREATE INDEX IF NOT EXISTS idx_torrents_info_hash_source_path ON torrents(info_hash, source_path);
   184|            CREATE INDEX IF NOT EXISTS idx_torrents_source_path ON torrents(source_path);
   185|
   186|            CREATE TABLE IF NOT EXISTS torrent_directories (
   187|                id INTEGER PRIMARY KEY AUTOINCREMENT,
   188|                torrent_id INTEGER NOT NULL,
   189|                parent_id INTEGER,
   190|                name TEXT NOT NULL,
   191|                FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,
   192|                FOREIGN KEY (parent_id) REFERENCES torrent_directories(id) ON DELETE CASCADE
   193|            );
   194|
   195|            CREATE INDEX IF NOT EXISTS idx_torrent_dirs_torrent_id ON torrent_directories(torrent_id);
   196|            CREATE INDEX IF NOT EXISTS idx_torrent_dirs_parent_id ON torrent_directories(parent_id);
   197|
   198|            CREATE TABLE IF NOT EXISTS directory_closure (
   199|                ancestor_id INTEGER NOT NULL,
   200|                descendant_id INTEGER NOT NULL,
   201|                depth INTEGER NOT NULL,
   202|                PRIMARY KEY (ancestor_id, descendant_id),
   203|                FOREIGN KEY (ancestor_id) REFERENCES torrent_directories(id) ON DELETE CASCADE,
   204|                FOREIGN KEY (descendant_id) REFERENCES torrent_directories(id) ON DELETE CASCADE
   205|            );
   206|
   207|            CREATE INDEX IF NOT EXISTS idx_closure_descendant ON directory_closure(descendant_id);
   208|
   209|            CREATE TABLE IF NOT EXISTS torrent_files (
   210|                id INTEGER PRIMARY KEY AUTOINCREMENT,
   211|                torrent_id INTEGER NOT NULL,
   212|                directory_id INTEGER,
   213|                name TEXT NOT NULL,
   214|                path TEXT NOT NULL DEFAULT '',
   215|                size INTEGER NOT NULL,
   216|                first_piece INTEGER NOT NULL DEFAULT 0,
   217|                last_piece INTEGER NOT NULL DEFAULT 0,
   218|                piece_start INTEGER,
   219|                piece_end INTEGER,
   220|                FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,
   221|                FOREIGN KEY (directory_id) REFERENCES torrent_directories(id) ON DELETE CASCADE
   222|            );
   223|
   224|            CREATE INDEX IF NOT EXISTS idx_torrent_files_torrent_id ON torrent_files(torrent_id);
   225|            CREATE INDEX IF NOT EXISTS idx_torrent_files_directory_id ON torrent_files(directory_id);
   226|            CREATE INDEX IF NOT EXISTS idx_torrent_files_path ON torrent_files(path);",
   227|        )?;
   228|        Ok(())
   229|    }
   230|
   231|    fn migrate_v2(conn: &Connection) -> Result<(), DbError> {
   232|        conn.execute_batch(
   233|            "ALTER TABLE torrents ADD COLUMN file_count INTEGER NOT NULL DEFAULT 1;
   234|             ALTER TABLE torrents ADD COLUMN status TEXT NOT NULL DEFAULT 'pending';
   235|             ALTER TABLE torrents ADD COLUMN torrent_data BLOB;
   236|             ALTER TABLE torrents ADD COLUMN resume_data BLOB;
   237|
   238|             CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status);
   239|             CREATE INDEX IF NOT EXISTS idx_torrents_info_hash_source_path ON torrents(info_hash, source_path);
   240|
   241|             ALTER TABLE torrent_files ADD COLUMN path TEXT NOT NULL DEFAULT '';
   242|             ALTER TABLE torrent_files ADD COLUMN first_piece INTEGER NOT NULL DEFAULT 0;
   243|             ALTER TABLE torrent_files ADD COLUMN last_piece INTEGER NOT NULL DEFAULT 0;
   244|
   245|             CREATE INDEX IF NOT EXISTS idx_torrent_files_path ON torrent_files(path);",
   246|        )?;
   247|        Ok(())
   248|    }
   249|
   250|    fn migrate_v3(conn: &Connection) -> Result<(), DbError> {
   251|        conn.execute_batch(
   252|            "CREATE TABLE IF NOT EXISTS metadata_directories (
   253|                id INTEGER PRIMARY KEY AUTOINCREMENT,
   254|                parent_id INTEGER,
   255|                name TEXT NOT NULL,
   256|                path TEXT NOT NULL UNIQUE,
   257|                FOREIGN KEY (parent_id) REFERENCES metadata_directories(id) ON DELETE CASCADE
   258|            );
   259|
   260|            CREATE INDEX IF NOT EXISTS idx_metadata_dirs_parent_id ON metadata_directories(parent_id);
   261|            CREATE INDEX IF NOT EXISTS idx_metadata_dirs_path ON metadata_directories(path);
   262|
   263|            CREATE TABLE IF NOT EXISTS metadata_directory_closure (
   264|                ancestor_id INTEGER NOT NULL,
   265|                descendant_id INTEGER NOT NULL,
   266|                depth INTEGER NOT NULL,
   267|                PRIMARY KEY (ancestor_id, descendant_id),
   268|                FOREIGN KEY (ancestor_id) REFERENCES metadata_directories(id) ON DELETE CASCADE,
   269|                FOREIGN KEY (descendant_id) REFERENCES metadata_directories(id) ON DELETE CASCADE
   270|            );
   271|
   272|            CREATE INDEX IF NOT EXISTS idx_metadata_closure_descendant ON metadata_directory_closure(descendant_id);",
   273|        )?;
   274|        Ok(())
   275|    }
   276|
   277|    fn migrate_v4(conn: &Connection) -> Result<(), DbError> {
   278|        conn.execute_batch(
   279|            "ALTER TABLE torrents ADD COLUMN filename TEXT NOT NULL DEFAULT '';
   280|             UPDATE torrents SET filename = name WHERE filename = '';",
   281|        )?;
   282|        Ok(())
   283|    }
   284|
   285|    pub fn rebuild_metadata_directories(&mut self) -> Result<(), DbError> {
   286|        let paths: Vec<String> = {
   287|            let mut stmt = self.conn.prepare(
   288|                "SELECT DISTINCT source_path FROM torrents WHERE source_path != ''",
   289|            )?;
   290|            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
   291|            rows.collect::<Result<Vec<_>, _>>()?
   292|        };
   293|
   294|        for path in paths {
   295|            self.ensure_metadata_directories(&path)?;
   296|        }
   297|
   298|        Ok(())
   299|    }
   300|
   301|    pub fn insert_torrent(
   302|        &mut self,
   303|        source_path: &str,
   304|        name: &str,
   305|        filename: &str,
   306|        total_size: i64,
   307|        info_hash: &str,
   308|        file_count: i64,
   309|    ) -> Result<InsertTorrentResult, DbError> {
   310|        let existing: Option<i64> = self
   311|            .conn
   312|            .query_row(
   313|                "SELECT id FROM torrents WHERE info_hash = ? AND source_path = ?",
   314|                params![info_hash, source_path],
   315|                |row| row.get(0),
   316|            )
   317|            .optional()?
   318|            .flatten();
   319|
   320|        if let Some(id) = existing {
   321|            return Ok(InsertTorrentResult::Duplicate(id));
   322|        }
   323|
   324|        self.conn.execute(
   325|            "INSERT INTO torrents (source_path, name, filename, total_size, info_hash, file_count, status) VALUES (?, ?, ?, ?, ?, ?, 'pending')",
   326|            params![source_path, name, filename, total_size, info_hash, file_count],
   327|        )?;
   328|
   329|        let id = self.conn.last_insert_rowid();
   330|        
   331|        if !source_path.is_empty() {
   332|            if let Err(e) = self.ensure_metadata_directories(source_path) {
   333|                tracing::warn!("Failed to create metadata directories for {}: {}", source_path, e);
   334|            }
   335|        }
   336|        
   337|        Ok(InsertTorrentResult::Inserted(id))
   338|    }
   339|
   340|    fn ensure_metadata_directories(&mut self, source_path: &str) -> Result<(), DbError> {
   341|        let parts: Vec<&str> = source_path.split('/').filter(|s| !s.is_empty()).collect();
   342|        if parts.is_empty() {
   343|            return Ok(());
   344|        }
   345|
   346|        let mut current_path = String::new();
   347|        let mut parent_id: Option<i64> = None;
   348|
   349|        for part in parts {
   350|            if current_path.is_empty() {
   351|                current_path = part.to_string();
   352|            } else {
   353|                current_path = format!("{}/{}", current_path, part);
   354|            }
   355|
   356|            let existing_id: Option<i64> = self.conn
   357|                .query_row(
   358|                    "SELECT id FROM metadata_directories WHERE path = ?",
   359|                    params![&current_path],
   360|                    |row| row.get(0),
   361|                )
   362|                .optional()?
   363|                .flatten();
   364|
   365|            if let Some(id) = existing_id {
   366|                parent_id = Some(id);
   367|                continue;
   368|            }
   369|
   370|            self.conn.execute(
   371|                "INSERT INTO metadata_directories (parent_id, name, path) VALUES (?, ?, ?)",
   372|                params![parent_id, part, &current_path],
   373|            )?;
   374|            let dir_id = self.conn.last_insert_rowid();
   375|
   376|            self.conn.execute(
   377|                "INSERT INTO metadata_directory_closure (ancestor_id, descendant_id, depth) VALUES (?, ?, 0)",
   378|                params![dir_id, dir_id],
   379|            )?;
   380|
   381|            if let Some(pid) = parent_id {
   382|                self.conn.execute(
   383|                    "INSERT INTO metadata_directory_closure (ancestor_id, descendant_id, depth)
   384|                     SELECT ancestor_id, ?, depth + 1 FROM metadata_directory_closure WHERE descendant_id = ?",
   385|                    params![dir_id, pid],
   386|                )?;
   387|            }
   388|
   389|            parent_id = Some(dir_id);
   390|        }
   391|
   392|        Ok(())
   393|    }
   394|
   395|    pub fn set_torrent_data(&mut self, torrent_id: i64, data: &[u8]) -> Result<(), DbError> {
   396|        self.conn.execute(
   397|            "UPDATE torrents SET torrent_data = ? WHERE id = ?",
   398|            params![data, torrent_id],
   399|        )?;
   400|        Ok(())
   401|    }
   402|
   403|    pub fn set_resume_data(&mut self, torrent_id: i64, data: &[u8]) -> Result<(), DbError> {
   404|        self.conn.execute(
   405|            "UPDATE torrents SET resume_data = ? WHERE id = ?",
   406|            params![data, torrent_id],
   407|        )?;
   408|        Ok(())
   409|    }
   410|
   411|    pub fn set_torrent_status(&mut self, torrent_id: i64, status: &TorrentStatus) -> Result<(), DbError> {
   412|        self.conn.execute(
   413|            "UPDATE torrents SET status = ? WHERE id = ?",
   414|            params![status.as_str(), torrent_id],
   415|        )?;
   416|        Ok(())
   417|    }
   418|
   419|    pub fn insert_files(
   420|        &mut self,
   421|        torrent_id: i64,
   422|        files: &[FileEntry],
   423|    ) -> Result<(), DbError> {
   424|        let tx = self.conn.transaction()?;
   425|
   426|        let mut dir_cache: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
   427|
   428|        for file_entry in files {
   429|            let path_parts: Vec<&str> = file_entry.path.split('/').collect();
   430|            if path_parts.is_empty() {
   431|                continue;
   432|            }
   433|
   434|            let mut current_parent_id: Option<i64> = None;
   435|
   436|            for (i, part) in path_parts.iter().enumerate() {
   437|                let is_file = i == path_parts.len() - 1;
   438|                let current_path = path_parts[..=i].join("/");
   439|
   440|                if is_file {
   441|                    tx.execute(
   442|                        "INSERT INTO torrent_files (torrent_id, directory_id, name, path, size) VALUES (?, ?, ?, ?, ?)",
   443|                        params![torrent_id, current_parent_id, part, &file_entry.path, file_entry.size],
   444|                    )?;
   445|                } else {
   446|                    if let Some(&cached_id) = dir_cache.get(&current_path) {
   447|                        current_parent_id = Some(cached_id);
   448|                        continue;
   449|                    }
   450|
   451|                    let existing_id: Option<i64> = tx
   452|                        .query_row(
   453|                            "SELECT id FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
   454|                            params![torrent_id, current_parent_id, part],
   455|                            |row| row.get(0),
   456|                        )
   457|                        .optional()?
   458|                        .flatten();
   459|
   460|                    if let Some(id) = existing_id {
   461|                        dir_cache.insert(current_path.clone(), id);
   462|                        current_parent_id = Some(id);
   463|                        continue;
   464|                    }
   465|
   466|                    tx.execute(
   467|                        "INSERT INTO torrent_directories (torrent_id, parent_id, name) VALUES (?, ?, ?)",
   468|                        params![torrent_id, current_parent_id, part],
   469|                    )?;
   470|                    let dir_id = tx.last_insert_rowid();
   471|
   472|                    tx.execute(
   473|                        "INSERT INTO directory_closure (ancestor_id, descendant_id, depth) VALUES (?, ?, 0)",
   474|                        params![dir_id, dir_id],
   475|                    )?;
   476|
   477|                    if let Some(parent_id) = current_parent_id {
   478|                        tx.execute(
   479|                            "INSERT INTO directory_closure (ancestor_id, descendant_id, depth)
   480|                             SELECT ancestor_id, ?, depth + 1 FROM directory_closure WHERE descendant_id = ?",
   481|                            params![dir_id, parent_id],
   482|                        )?;
   483|                    }
   484|
   485|                    dir_cache.insert(current_path.clone(), dir_id);
   486|                    current_parent_id = Some(dir_id);
   487|                }
   488|            }
   489|        }
   490|
   491|        tx.commit()?;
   492|        Ok(())
   493|    }
   494|
   495|    /// Insert torrent and its files atomically in a single transaction.
   496|    /// This prevents orphaned torrent records without file entries.
   497|    pub fn insert_torrent_with_files(
   498|        &mut self,
   499|        source_path: &str,
   500|        name: &str,
   501|        total_size: i64,
   502|        info_hash: &str,
   503|        file_count: i64,
   504|        files: &[FileEntry],
   505|    ) -> Result<InsertTorrentResult, DbError> {
   506|        let tx = self.conn.transaction()?;
   507|
   508|        // Check for existing torrent with same info_hash and source_path
   509|        let existing: Option<i64> = tx
   510|            .query_row(
   511|                "SELECT id FROM torrents WHERE info_hash = ? AND source_path = ?",
   512|                params![info_hash, source_path],
   513|                |row| row.get(0),
   514|            )
   515|            .optional()?
   516|            .flatten();
   517|
   518|        if let Some(id) = existing {
   519|            return Ok(InsertTorrentResult::Duplicate(id));
   520|        }
   521|
   522|        // Insert torrent record
   523|        tx.execute(
   524|            "INSERT INTO torrents (source_path, name, total_size, info_hash, file_count, status) VALUES (?, ?, ?, ?, ?, 'pending')",
   525|            params![source_path, name, total_size, info_hash, file_count],
   526|        )?;
   527|        let torrent_id = tx.last_insert_rowid();
   528|
   529|        // Insert files in the same transaction
   530|        let mut dir_cache: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
   531|
   532|        for file_entry in files {
   533|            let path_parts: Vec<&str> = file_entry.path.split('/').collect();
   534|            if path_parts.is_empty() {
   535|                continue;
   536|            }
   537|
   538|            let mut current_parent_id: Option<i64> = None;
   539|
   540|            for (i, part) in path_parts.iter().enumerate() {
   541|                let is_file = i == path_parts.len() - 1;
   542|                let current_path = path_parts[..=i].join("/");
   543|
   544|                if is_file {
   545|                    tx.execute(
   546|                        "INSERT INTO torrent_files (torrent_id, directory_id, name, size) VALUES (?, ?, ?, ?)",
   547|                        params![torrent_id, current_parent_id, part, file_entry.size],
   548|                    )?;
   549|                } else {
   550|                    if let Some(&cached_id) = dir_cache.get(&current_path) {
   551|                        current_parent_id = Some(cached_id);
   552|                        continue;
   553|                    }
   554|
   555|                    let existing_id: Option<i64> = tx
   556|                        .query_row(
   557|                            "SELECT id FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
   558|                            params![torrent_id, current_parent_id, part],
   559|                            |row| row.get(0),
   560|                        )
   561|                        .optional()?
   562|                        .flatten();
   563|
   564|                    if let Some(id) = existing_id {
   565|                        dir_cache.insert(current_path.clone(), id);
   566|                        current_parent_id = Some(id);
   567|                        continue;
   568|                    }
   569|
   570|                    tx.execute(
   571|                        "INSERT INTO torrent_directories (torrent_id, parent_id, name) VALUES (?, ?, ?)",
   572|                        params![torrent_id, current_parent_id, part],
   573|                    )?;
   574|                    let dir_id = tx.last_insert_rowid();
   575|
   576|                    tx.execute(
   577|                        "INSERT INTO directory_closure (ancestor_id, descendant_id, depth) VALUES (?, ?, 0)",
   578|                        params![dir_id, dir_id],
   579|                    )?;
   580|
   581|                    if let Some(parent_id) = current_parent_id {
   582|                        tx.execute(
   583|                            "INSERT INTO directory_closure (ancestor_id, descendant_id, depth)
   584|                             SELECT ancestor_id, ?, depth + 1 FROM directory_closure WHERE descendant_id = ?",
   585|                            params![dir_id, parent_id],
   586|                        )?;
   587|                    }
   588|
   589|                    dir_cache.insert(current_path.clone(), dir_id);
   590|                    current_parent_id = Some(dir_id);
   591|                }
   592|            }
   593|        }
   594|
   595|        tx.commit()?;
   596|        Ok(InsertTorrentResult::Inserted(torrent_id))
   597|    }
   598|
   599|    pub fn get_torrent_by_source_path(&self, source_path: &str) -> Result<Option<Torrent>, DbError> {
   600|        let result = self
   601|            .conn
   602|            .query_row(
   603|                "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
   604|                 FROM torrents WHERE source_path = ?",
   605|                params![source_path],
   606|                |row| {
   607|                    Ok(Torrent {
   608|                        id: row.get(0)?,
   609|                        source_path: row.get(1)?,
   610|                        name: row.get(2)?,
   611|                        filename: row.get(3)?,
   612|                        total_size: row.get(4)?,
   613|                        info_hash: row.get(5)?,
   614|                        file_count: row.get(6)?,
   615|                        status: row.get::<_, String>(7)?.into(),
   616|                        torrent_data: row.get(8)?,
   617|                        resume_data: row.get(9)?,
   618|                        created_at: row.get(10)?,
   619|                    })
   620|                },
   621|            )
   622|            .optional()?;
   623|
   624|        Ok(result)
   625|    }
   626|
   627|    pub fn get_torrent_by_info_hash(&self, info_hash: &str) -> Result<Option<Torrent>, DbError> {
   628|        let result = self
   629|            .conn
   630|            .query_row(
   631|                "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
   632|                 FROM torrents WHERE info_hash = ?",
   633|                params![info_hash],
   634|                |row| {
   635|                    Ok(Torrent {
   636|                        id: row.get(0)?,
   637|                        source_path: row.get(1)?,
   638|                        name: row.get(2)?,
   639|                        filename: row.get(3)?,
   640|                        total_size: row.get(4)?,
   641|                        info_hash: row.get(5)?,
   642|                        file_count: row.get(6)?,
   643|                        status: row.get::<_, String>(7)?.into(),
   644|                        torrent_data: row.get(8)?,
   645|                        resume_data: row.get(9)?,
   646|                        created_at: row.get(10)?,
   647|                    })
   648|                },
   649|            )
   650|            .optional()?;
   651|
   652|        Ok(result)
   653|    }
   654|
   655|    pub fn get_files_by_torrent_id(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, DbError> {
   656|        let mut stmt = self.conn.prepare(
   657|            "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
   658|             FROM torrent_files WHERE torrent_id = ? ORDER BY id",
   659|        )?;
   660|
   661|        let files = stmt
   662|            .query_map(params![torrent_id], |row| {
   663|                Ok(TorrentFile {
   664|                    id: row.get(0)?,
   665|                    torrent_id: row.get(1)?,
   666|                    directory_id: row.get(2)?,
   667|                    name: row.get(3)?,
   668|                    path: row.get(4)?,
   669|                    size: row.get(5)?,
   670|                    first_piece: row.get(6)?,
   671|                    last_piece: row.get(7)?,
   672|                    piece_start: row.get(8)?,
   673|                    piece_end: row.get(9)?,
   674|                })
   675|            })?
   676|            .collect::<Result<Vec<_>, _>>()?;
   677|
   678|        Ok(files)
   679|    }
   680|
   681|    pub fn get_subdirectory_ids(&self, parent_id: i64) -> Result<Vec<i64>, DbError> {
   682|        let mut stmt = self.conn.prepare(
   683|            "SELECT id FROM torrent_directories WHERE parent_id = ?",
   684|        )?;
   685|
   686|        let ids = stmt
   687|            .query_map(params![parent_id], |row| row.get(0))?
   688|            .collect::<Result<Vec<_>, _>>()?;
   689|
   690|        Ok(ids)
   691|    }
   692|
   693|    pub fn get_files_in_directory(&self, directory_id: i64) -> Result<Vec<TorrentFile>, DbError> {
   694|        let mut stmt = self.conn.prepare(
   695|            "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
   696|             FROM torrent_files WHERE directory_id = ?",
   697|        )?;
   698|
   699|        let files = stmt
   700|            .query_map(params![directory_id], |row| {
   701|                Ok(TorrentFile {
   702|                    id: row.get(0)?,
   703|                    torrent_id: row.get(1)?,
   704|                    directory_id: row.get(2)?,
   705|                    name: row.get(3)?,
   706|                    path: row.get(4)?,
   707|                    size: row.get(5)?,
   708|                    first_piece: row.get(6)?,
   709|                    last_piece: row.get(7)?,
   710|                    piece_start: row.get(8)?,
   711|                    piece_end: row.get(9)?,
   712|                })
   713|            })?
   714|            .collect::<Result<Vec<_>, _>>()?;
   715|
   716|        Ok(files)
   717|    }
   718|
   719|    pub fn get_root_files(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, DbError> {
   720|        let mut stmt = self.conn.prepare(
   721|            "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
   722|             FROM torrent_files WHERE torrent_id = ? AND directory_id IS NULL",
   723|        )?;
   724|
   725|        let files = stmt
   726|            .query_map(params![torrent_id], |row| {
   727|                Ok(TorrentFile {
   728|                    id: row.get(0)?,
   729|                    torrent_id: row.get(1)?,
   730|                    directory_id: row.get(2)?,
   731|                    name: row.get(3)?,
   732|                    path: row.get(4)?,
   733|                    size: row.get(5)?,
   734|                    first_piece: row.get(6)?,
   735|                    last_piece: row.get(7)?,
   736|                    piece_start: row.get(8)?,
   737|                    piece_end: row.get(9)?,
   738|                })
   739|            })?
   740|            .collect::<Result<Vec<_>, _>>()?;
   741|
   742|        Ok(files)
   743|    }
   744|
   745|    pub fn get_torrent_directory(&self, torrent_id: i64, parent_id: Option<i64>, name: &str) -> Result<Option<TorrentDirectory>, DbError> {
   746|        let result = self.conn
   747|            .query_row(
   748|                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
   749|                params![torrent_id, parent_id, name],
   750|                |row| {
   751|                    Ok(TorrentDirectory {
   752|                        id: row.get(0)?,
   753|                        torrent_id: row.get(1)?,
   754|                        parent_id: row.get(2)?,
   755|                        name: row.get(3)?,
   756|                    })
   757|                },
   758|            )
   759|            .optional()?;
   760|
   761|        Ok(result)
   762|    }
   763|
   764|    pub fn get_torrent_directory_by_id(&self, dir_id: i64) -> Result<Option<TorrentDirectory>, DbError> {
   765|        let result = self.conn
   766|            .query_row(
   767|                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE id = ?",
   768|                params![dir_id],
   769|                |row| {
   770|                    Ok(TorrentDirectory {
   771|                        id: row.get(0)?,
   772|                        torrent_id: row.get(1)?,
   773|                        parent_id: row.get(2)?,
   774|                        name: row.get(3)?,
   775|                    })
   776|                },
   777|            )
   778|            .optional()?;
   779|
   780|        Ok(result)
   781|    }
   782|
   783|    pub fn get_torrent_directories_by_parent(&self, parent_id: Option<i64>, torrent_id: i64) -> Result<Vec<TorrentDirectory>, DbError> {
   784|        let mut stmt = if parent_id.is_none() {
   785|            self.conn.prepare(
   786|                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE torrent_id = ? AND parent_id IS NULL",
   787|            )?
   788|        } else {
   789|            self.conn.prepare(
   790|                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE torrent_id = ? AND parent_id = ?",
   791|            )?
   792|        };
   793|
   794|        let dirs = if parent_id.is_none() {
   795|            stmt.query_map(params![torrent_id], |row| {
   796|                Ok(TorrentDirectory {
   797|                    id: row.get(0)?,
   798|                    torrent_id: row.get(1)?,
   799|                    parent_id: row.get(2)?,
   800|                    name: row.get(3)?,
   801|                })
   802|            })?
   803|            .collect::<Result<Vec<_>, _>>()?
   804|        } else {
   805|            stmt.query_map(params![torrent_id, parent_id], |row| {
   806|                Ok(TorrentDirectory {
   807|                    id: row.get(0)?,
   808|                    torrent_id: row.get(1)?,
   809|                    parent_id: row.get(2)?,
   810|                    name: row.get(3)?,
   811|                })
   812|            })?
   813|            .collect::<Result<Vec<_>, _>>()?
   814|        };
   815|
   816|        Ok(dirs)
   817|    }
   818|
   819|    pub fn get_all_files_under_directory(&self, directory_id: i64) -> Result<Vec<TorrentFile>, DbError> {
   820|        let mut stmt = self.conn.prepare(
   821|            "SELECT f.id, f.torrent_id, f.directory_id, f.name, f.path, f.size, f.first_piece, f.last_piece, f.piece_start, f.piece_end
   822|             FROM torrent_files f
   823|             JOIN directory_closure c ON f.directory_id = c.descendant_id
   824|             WHERE c.ancestor_id = ?",
   825|        )?;
   826|
   827|        let files = stmt
   828|            .query_map(params![directory_id], |row| {
   829|                Ok(TorrentFile {
   830|                    id: row.get(0)?,
   831|                    torrent_id: row.get(1)?,
   832|                    directory_id: row.get(2)?,
   833|                    name: row.get(3)?,
   834|                    path: row.get(4)?,
   835|                    size: row.get(5)?,
   836|                    first_piece: row.get(6)?,
   837|                    last_piece: row.get(7)?,
   838|                    piece_start: row.get(8)?,
   839|                    piece_end: row.get(9)?,
   840|                })
   841|            })?
   842|            .collect::<Result<Vec<_>, _>>()?;
   843|
   844|        Ok(files)
   845|    }
   846|
   847|    pub fn delete_torrent(&mut self, torrent_id: i64) -> Result<(), DbError> {
   848|        self.conn.execute(
   849|            "DELETE FROM torrents WHERE id = ?",
   850|            params![torrent_id],
   851|        )?;
   852|        Ok(())
   853|    }
   854|
   855|    pub fn get_all_torrents(&self) -> Result<Vec<Torrent>, DbError> {
   856|        let mut stmt = self.conn.prepare(
   857|            "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
   858|             FROM torrents ORDER BY id",
   859|        )?;
   860|
   861|        let torrents = stmt
   862|            .query_map([], |row| {
   863|                Ok(Torrent {
   864|                    id: row.get(0)?,
   865|                    source_path: row.get(1)?,
   866|                    name: row.get(2)?,
   867|                    filename: row.get(3)?,
   868|                    total_size: row.get(4)?,
   869|                    info_hash: row.get(5)?,
   870|                    file_count: row.get(6)?,
   871|                    status: row.get::<_, String>(7)?.into(),
   872|                    torrent_data: row.get(8)?,
   873|                    resume_data: row.get(9)?,
   874|                    created_at: row.get(10)?,
   875|                })
   876|            })?
   877|            .collect::<Result<Vec<_>, _>>()?;
   878|
   879|        Ok(torrents)
   880|    }
   881|
   882|    pub fn get_torrents_by_status(&self, status: &TorrentStatus) -> Result<Vec<Torrent>, DbError> {
   883|        let mut stmt = self.conn.prepare(
   884|            "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
   885|             FROM torrents WHERE status = ? ORDER BY id",
   886|        )?;
   887|
   888|        let torrents = stmt
   889|            .query_map(params![status.as_str()], |row| {
   890|                Ok(Torrent {
   891|                    id: row.get(0)?,
   892|                    source_path: row.get(1)?,
   893|                    name: row.get(2)?,
   894|                    filename: row.get(3)?,
   895|                    total_size: row.get(4)?,
   896|                    info_hash: row.get(5)?,
   897|                    file_count: row.get(6)?,
   898|                    status: row.get::<_, String>(7)?.into(),
   899|                    torrent_data: row.get(8)?,
   900|                    resume_data: row.get(9)?,
   901|                    created_at: row.get(10)?,
   902|                })
   903|            })?
   904|            .collect::<Result<Vec<_>, _>>()?;
   905|
   906|        Ok(torrents)
   907|    }
   908|
   909|    pub fn get_torrents_by_source_path(&self, source_path: &str) -> Result<Vec<Torrent>, DbError> {
   910|        let mut stmt = self.conn.prepare(
   911|            "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
   912|             FROM torrents WHERE source_path = ? ORDER BY id",
   913|        )?;
   914|
   915|        let torrents = stmt
   916|            .query_map(params![source_path], |row| {
   917|                Ok(Torrent {
   918|                    id: row.get(0)?,
   919|                    source_path: row.get(1)?,
   920|                    name: row.get(2)?,
   921|                    filename: row.get(3)?,
   922|                    total_size: row.get(4)?,
   923|                    info_hash: row.get(5)?,
   924|                    file_count: row.get(6)?,
   925|                    status: row.get::<_, String>(7)?.into(),
   926|                    torrent_data: row.get(8)?,
   927|                    resume_data: row.get(9)?,
   928|                    created_at: row.get(10)?,
   929|                })
   930|            })?
   931|            .collect::<Result<Vec<_>, _>>()?;
   932|
   933|        Ok(torrents)
   934|    }
   935|
   936|    /// Get all metadata directories with their id, parent_id, name, and path.
   937|    /// Used to restore inode cache on filesystem startup.
   938|    pub fn get_all_metadata_directories(&self) -> Result<Vec<(i64, Option<i64>, String, String)>, DbError> {
   939|        let mut stmt = self.conn.prepare(
   940|            "SELECT id, parent_id, name, path FROM metadata_directories ORDER BY path",
   941|        )?;
   942|        let rows = stmt.query_map([], |row| {
   943|            Ok((
   944|                row.get(0)?,
   945|                row.get(1)?,
   946|                row.get(2)?,
   947|                row.get(3)?,
   948|            ))
   949|        })?;
   950|        Ok(rows.collect::<Result<Vec<_>, _>>()?)
   951|    }
   952|
   953|    pub fn get_source_path_prefixes(&self, prefix: &str) -> Result<Vec<String>, DbError> {
   954|        let names: Vec<String> = if prefix.is_empty() {
   955|            let mut stmt = self.conn.prepare(
   956|                "SELECT name FROM metadata_directories WHERE parent_id IS NULL ORDER BY name",
   957|            )?;
   958|            let rows = stmt.query_map([], |row| row.get(0))?;
   959|            rows.collect::<Result<Vec<_>, _>>()?
   960|        } else {
   961|            let parent_id: Option<i64> = self.conn
   962|                .query_row(
   963|                    "SELECT id FROM metadata_directories WHERE path = ?",
   964|                    params![prefix],
   965|                    |row| row.get(0),
   966|                )
   967|                .optional()?
   968|                .flatten();
   969|
   970|            match parent_id {
   971|                Some(pid) => {
   972|                    let mut stmt = self.conn.prepare(
   973|                        "SELECT name FROM metadata_directories WHERE parent_id = ? ORDER BY name",
   974|                    )?;
   975|                    let rows = stmt.query_map(params![pid], |row| row.get(0))?;
   976|                    rows.collect::<Result<Vec<_>, _>>()?
   977|                }
   978|                None => Vec::new(),
   979|            }
   980|        };
   981|
   982|        Ok(names)
   983|    }
   984|
   985|    /// Get all metadata directories ordered by path depth (parent directories first).
   986|    /// Depth is computed from the path by counting path separators.
   987|    /// This ensures that when restoring inodes, parent directories are created before children.
   988|    pub fn get_all_metadata_dirs_ordered(&self) -> Result<Vec<(i64, Option<i64>, String, String)>, DbError> {
   989|        let mut stmt = self.conn.prepare(
   990|            "SELECT md.id, md.parent_id, md.name, md.path
   991|             FROM metadata_directories md
   992|             ORDER BY (LENGTH(md.path) - LENGTH(REPLACE(md.path, '/', ''))), md.path",
   993|        )?;
   994|        
   995|        let rows = stmt.query_map([], |row| {
   996|            Ok((
   997|                row.get::<_, i64>(0)?,           // id
   998|                row.get::<_, Option<i64>>(1)?,   // parent_id
   999|                row.get::<_, String>(2)?,        // name
  1000|                row.get::<_, String>(3)?,        // path
  1001|            ))
  1002|        })?;
  1003|        
  1004|        rows.collect::<Result<Vec<_>, _>>().map_err(DbError::from)
  1005|    }
  1006|
  1007|    pub fn get_file_by_path(&self, torrent_id: i64, path: &str) -> Result<Option<TorrentFile>, DbError> {
  1008|        let parts: Vec<&str> = path.split('/').collect();
  1009|        if parts.is_empty() {
  1010|            return Ok(None);
  1011|        }
  1012|
  1013|        let file_name = parts.last().unwrap();
  1014|        let dir_path_parts: Vec<&str> = parts[..parts.len()-1].to_vec();
  1015|        
  1016|        if dir_path_parts.is_empty() {
  1017|            let result = self.conn
  1018|                .query_row(
  1019|                    "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
  1020|                     FROM torrent_files WHERE torrent_id = ? AND directory_id IS NULL AND name = ?",
  1021|                    params![torrent_id, file_name],
  1022|                    |row| {
  1023|                        Ok(TorrentFile {
  1024|                            id: row.get(0)?,
  1025|                            torrent_id: row.get(1)?,
  1026|                            directory_id: row.get(2)?,
  1027|                            name: row.get(3)?,
  1028|                            path: row.get(4)?,
  1029|                            size: row.get(5)?,
  1030|                            first_piece: row.get(6)?,
  1031|                            last_piece: row.get(7)?,
  1032|                            piece_start: row.get(8)?,
  1033|                            piece_end: row.get(9)?,
  1034|                        })
  1035|                    },
  1036|                )
  1037|                .optional()?;
  1038|
  1039|            return Ok(result);
  1040|        }
  1041|
  1042|        let dir_id = self.resolve_directory_path(torrent_id, &dir_path_parts)?;
  1043|        
  1044|        match dir_id {
  1045|            Some(did) => {
  1046|                let result = self.conn
  1047|                    .query_row(
  1048|                        "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
  1049|                         FROM torrent_files WHERE torrent_id = ? AND directory_id = ? AND name = ?",
  1050|                        params![torrent_id, did, file_name],
  1051|                        |row| {
  1052|                            Ok(TorrentFile {
  1053|                                id: row.get(0)?,
  1054|                                torrent_id: row.get(1)?,
  1055|                                directory_id: row.get(2)?,
  1056|                                name: row.get(3)?,
  1057|                                path: row.get(4)?,
  1058|                                size: row.get(5)?,
  1059|                                first_piece: row.get(6)?,
  1060|                                last_piece: row.get(7)?,
  1061|                                piece_start: row.get(8)?,
  1062|                                piece_end: row.get(9)?,
  1063|                            })
  1064|                        },
  1065|                    )
  1066|                    .optional()?;
  1067|                Ok(result)
  1068|            }
  1069|            None => Ok(None),
  1070|        }
  1071|    }
  1072|
  1073|    fn resolve_directory_path(&self, torrent_id: i64, parts: &[&str]) -> Result<Option<i64>, DbError> {
  1074|        let mut current_parent: Option<i64> = None;
  1075|
  1076|        for part in parts {
  1077|            let existing_id: Option<i64> = self.conn
  1078|                .query_row(
  1079|                    "SELECT id FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
  1080|                    params![torrent_id, current_parent, part],
  1081|                    |row| row.get(0),
  1082|                )
  1083|                .optional()?
  1084|                .flatten();
  1085|
  1086|            match existing_id {
  1087|                Some(id) => current_parent = Some(id),
  1088|                None => return Ok(None),
  1089|            }
  1090|        }
  1091|
  1092|        Ok(current_parent)
  1093|    }
  1094|
  1095|pub fn get_torrent_id_by_name_and_source_path(&self, name: &str, source_path: &str) -> Result<Option<i64>, DbError> {
  1096|        let result = self.conn
  1097|            .query_row(
  1098|                "SELECT id FROM torrents WHERE name = ? AND source_path = ?",
  1099|                params![name, source_path],
  1100|                |row| row.get(0),
  1101|            )
  1102|            .optional()?
  1103|            .flatten();
  1104|
  1105|        Ok(result)
  1106|    }
  1107|
  1108|    pub fn get_torrent_by_id(&self, id: i64) -> Result<Option<Torrent>, DbError> {
  1109|        let result = self
  1110|            .conn
  1111|            .query_row(
  1112|                "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
  1113|                 FROM torrents WHERE id = ?",
  1114|                params![id],
  1115|                |row| {
  1116|                    Ok(Torrent {
  1117|                        id: row.get(0)?,
  1118|                        source_path: row.get(1)?,
  1119|                        name: row.get(2)?,
  1120|                        filename: row.get(3)?,
  1121|                        total_size: row.get(4)?,
  1122|                        info_hash: row.get(5)?,
  1123|                        file_count: row.get(6)?,
  1124|                        status: row.get::<_, String>(7)?.into(),
  1125|                        torrent_data: row.get(8)?,
  1126|                        resume_data: row.get(9)?,
  1127|                        created_at: row.get(10)?,
  1128|                    })
  1129|                },
  1130|            )
  1131|            .optional()?;
  1132|
  1133|        Ok(result)
  1134|    }
  1135|}
  1136|
  1137|#[cfg(test)]
  1138|mod tests {
  1139|    use super::*;
  1140|    use tempfile::NamedTempFile;
  1141|
  1142|    #[test]
  1143|    fn test_open_in_memory() {
  1144|        let db = Database::open_in_memory();
  1145|        assert!(db.is_ok());
  1146|    }
  1147|
  1148|    #[test]
  1149|    fn test_insert_and_get_torrent() {
  1150|        let mut db = Database::open_in_memory().unwrap();
  1151|        
  1152|        let result = db.insert_torrent("test/path", "Test Torrent", "Test Torrent", 1024, "abc123", 5).unwrap();
  1153|        assert_eq!(result, InsertTorrentResult::Inserted(1));
  1154|        
  1155|        let torrent = db.get_torrent_by_source_path("test/path").unwrap().unwrap();
  1156|        assert_eq!(torrent.name, "Test Torrent");
  1157|        assert_eq!(torrent.total_size, 1024);
  1158|        assert_eq!(torrent.file_count, 5);
  1159|        assert_eq!(torrent.status, TorrentStatus::Pending);
  1160|    }
  1161|
  1162|    #[test]
  1163|    fn test_same_info_hash_different_source_path() {
  1164|        let mut db = Database::open_in_memory().unwrap();
  1165|        
  1166|        let result1 = db.insert_torrent("path1", "Torrent 1", "Torrent 1", 1024, "hash1", 1).unwrap();
  1167|        assert_eq!(result1, InsertTorrentResult::Inserted(1));
  1168|        
  1169|        let result2 = db.insert_torrent("path2", "Torrent 2", "Torrent 2", 2048, "hash1", 2).unwrap();
  1170|        assert_eq!(result2, InsertTorrentResult::Inserted(2));
  1171|        
  1172|        let torrent1 = db.get_torrent_by_source_path("path1").unwrap().unwrap();
  1173|        let torrent2 = db.get_torrent_by_source_path("path2").unwrap().unwrap();
  1174|        assert_eq!(torrent1.info_hash, torrent2.info_hash);
  1175|        assert_eq!(torrent1.id, 1);
  1176|        assert_eq!(torrent2.id, 2);
  1177|    }
  1178|
  1179|    #[test]
  1180|    fn test_duplicate_info_hash_and_source_path() {
  1181|        let mut db = Database::open_in_memory().unwrap();
  1182|        
  1183|        db.insert_torrent("path1", "Torrent 1", "Torrent 1", 1024, "hash1", 1).unwrap();
  1184|        let result = db.insert_torrent("path1", "Torrent 2", "Torrent 2", 2048, "hash1", 2).unwrap();
  1185|        assert_eq!(result, InsertTorrentResult::Duplicate(1));
  1186|    }
  1187|
  1188|    #[test]
  1189|    fn test_torrent_status() {
  1190|        let mut db = Database::open_in_memory().unwrap();
  1191|        
  1192|        let torrent_id = match db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 1).unwrap() {
  1193|            InsertTorrentResult::Inserted(id) => id,
  1194|            _ => panic!("Expected Inserted"),
  1195|        };
  1196|        
  1197|        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
  1198|        assert_eq!(torrent.status, TorrentStatus::Pending);
  1199|        
  1200|        db.set_torrent_status(torrent_id, &TorrentStatus::Downloading).unwrap();
  1201|        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
  1202|        assert_eq!(torrent.status, TorrentStatus::Downloading);
  1203|        
  1204|        db.set_torrent_status(torrent_id, &TorrentStatus::Seeding).unwrap();
  1205|        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
  1206|        assert_eq!(torrent.status, TorrentStatus::Seeding);
  1207|        
  1208|        db.set_torrent_status(torrent_id, &TorrentStatus::Error).unwrap();
  1209|        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
  1210|        assert_eq!(torrent.status, TorrentStatus::Error);
  1211|    }
  1212|
  1213|    #[test]
  1214|    fn test_torrent_data() {
  1215|        let mut db = Database::open_in_memory().unwrap();
  1216|        
  1217|        let torrent_id = match db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 1).unwrap() {
  1218|            InsertTorrentResult::Inserted(id) => id,
  1219|            _ => panic!("Expected Inserted"),
  1220|        };
  1221|        
  1222|        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
  1223|        assert!(torrent.torrent_data.is_none());
  1224|        assert!(torrent.resume_data.is_none());
  1225|        
  1226|        let test_data = vec![1, 2, 3, 4, 5];
  1227|        db.set_torrent_data(torrent_id, &test_data).unwrap();
  1228|        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
  1229|        assert_eq!(torrent.torrent_data, Some(test_data));
  1230|        
  1231|        let resume_data = vec![10, 20, 30];
  1232|        db.set_resume_data(torrent_id, &resume_data).unwrap();
  1233|        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
  1234|        assert_eq!(torrent.resume_data, Some(resume_data));
  1235|    }
  1236|
  1237|    #[test]
  1238|    fn test_get_torrents_by_status() {
  1239|        let mut db = Database::open_in_memory().unwrap();
  1240|        
  1241|        let id1 = match db.insert_torrent("path1", "T1", "T1", 100, "hash1", 1).unwrap() {
  1242|            InsertTorrentResult::Inserted(id) => id,
  1243|            _ => panic!("Expected Inserted"),
  1244|        };
  1245|        let id2 = match db.insert_torrent("path2", "T2", "T2", 200, "hash2", 1).unwrap() {
  1246|            InsertTorrentResult::Inserted(id) => id,
  1247|            _ => panic!("Expected Inserted"),
  1248|        };
  1249|        let id3 = match db.insert_torrent("path3", "T3", "T3", 300, "hash3", 1).unwrap() {
  1250|            InsertTorrentResult::Inserted(id) => id,
  1251|            _ => panic!("Expected Inserted"),
  1252|        };
  1253|        
  1254|        db.set_torrent_status(id1, &TorrentStatus::Downloading).unwrap();
  1255|        db.set_torrent_status(id2, &TorrentStatus::Seeding).unwrap();
  1256|        
  1257|        let pending = db.get_torrents_by_status(&TorrentStatus::Pending).unwrap();
  1258|        assert_eq!(pending.len(), 1);
  1259|        
  1260|        let downloading = db.get_torrents_by_status(&TorrentStatus::Downloading).unwrap();
  1261|        assert_eq!(downloading.len(), 1);
  1262|        
  1263|        let seeding = db.get_torrents_by_status(&TorrentStatus::Seeding).unwrap();
  1264|        assert_eq!(seeding.len(), 1);
  1265|    }
  1266|
  1267|    #[test]
  1268|    fn test_insert_files() {
  1269|        let mut db = Database::open_in_memory().unwrap();
  1270|        
  1271|        let torrent_id = match db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 3).unwrap() {
  1272|            InsertTorrentResult::Inserted(id) => id,
  1273|            _ => panic!("Expected Inserted"),
  1274|        };
  1275|
  1276|        let files = vec![
  1277|            FileEntry { path: "dir1/file1.txt".to_string(), size: 100 },
  1278|            FileEntry { path: "dir1/file2.txt".to_string(), size: 200 },
  1279|            FileEntry { path: "dir2/file3.txt".to_string(), size: 300 },
  1280|        ];
  1281|
  1282|        db.insert_files(torrent_id, &files).unwrap();
  1283|
  1284|        let all_files = db.get_files_by_torrent_id(torrent_id).unwrap();
  1285|        assert_eq!(all_files.len(), 3);
  1286|    }
  1287|
  1288|    #[test]
  1289|    fn test_file_path_field_populated() {
  1290|        let mut db = Database::open_in_memory().unwrap();
  1291|        
  1292|        let torrent_id = match db.insert_torrent("path1", "Test", 1024, "hash1", 3).unwrap() {
  1293|            InsertTorrentResult::Inserted(id) => id,
  1294|            _ => panic!("Expected Inserted"),
  1295|        };
  1296|
  1297|        let files = vec![
  1298|            FileEntry { path: "dir1/file1.txt".to_string(), size: 100 },
  1299|            FileEntry { path: "file2.txt".to_string(), size: 200 },
  1300|            FileEntry { path: "a/b/c/deep.txt".to_string(), size: 300 },
  1301|        ];
  1302|
  1303|        db.insert_files(torrent_id, &files).unwrap();
  1304|
  1305|        let all_files = db.get_files_by_torrent_id(torrent_id).unwrap();
  1306|        assert_eq!(all_files.len(), 3);
  1307|        
  1308|        // Verify path field is correctly populated
  1309|        let file1 = all_files.iter().find(|f| f.name == "file1.txt").unwrap();
  1310|        assert_eq!(file1.path, "dir1/file1.txt");
  1311|        
  1312|        let file2 = all_files.iter().find(|f| f.name == "file2.txt").unwrap();
  1313|        assert_eq!(file2.path, "file2.txt");
  1314|        
  1315|        let deep = all_files.iter().find(|f| f.name == "deep.txt").unwrap();
  1316|        assert_eq!(deep.path, "a/b/c/deep.txt");
  1317|    }
  1318|
  1319|    #[test]
  1320|    fn test_get_subdirectory_ids() {
  1321|        let mut db = Database::open_in_memory().unwrap();
  1322|        
  1323|        let torrent_id = match db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 2).unwrap() {
  1324|            InsertTorrentResult::Inserted(id) => id,
  1325|            _ => panic!("Expected Inserted"),
  1326|        };
  1327|
  1328|        let files = vec![
  1329|            FileEntry { path: "dir1/file1.txt".to_string(), size: 100 },
  1330|            FileEntry { path: "dir2/file2.txt".to_string(), size: 200 },
  1331|        ];
  1332|
  1333|        db.insert_files(torrent_id, &files).unwrap();
  1334|
  1335|        let root_dirs = db.get_torrent_directories_by_parent(None, torrent_id).unwrap();
  1336|        assert_eq!(root_dirs.len(), 2);
  1337|    }
  1338|
  1339|    #[test]
  1340|    fn test_delete_torrent_cascade() {
  1341|        let mut db = Database::open_in_memory().unwrap();
  1342|        
  1343|        let torrent_id = match db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 1).unwrap() {
  1344|            InsertTorrentResult::Inserted(id) => id,
  1345|            _ => panic!("Expected Inserted"),
  1346|        };
  1347|
  1348|        let files = vec![FileEntry { path: "file.txt".to_string(), size: 100 }];
  1349|        db.insert_files(torrent_id, &files).unwrap();
  1350|
  1351|        db.delete_torrent(torrent_id).unwrap();
  1352|
  1353|        let torrent = db.get_torrent_by_source_path("path1").unwrap();
  1354|        assert!(torrent.is_none());
  1355|
  1356|        let files = db.get_files_by_torrent_id(torrent_id).unwrap();
  1357|        assert!(files.is_empty());
  1358|    }
  1359|
  1360|    #[test]
  1361|    fn test_get_files_in_directory() {
  1362|        let mut db = Database::open_in_memory().unwrap();
  1363|        
  1364|        let torrent_id = match db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 3).unwrap() {
  1365|            InsertTorrentResult::Inserted(id) => id,
  1366|            _ => panic!("Expected Inserted"),
  1367|        };
  1368|
  1369|        let files = vec![
  1370|            FileEntry { path: "dir1/file1.txt".to_string(), size: 100 },
  1371|            FileEntry { path: "dir1/file2.txt".to_string(), size: 200 },
  1372|            FileEntry { path: "file3.txt".to_string(), size: 300 },
  1373|        ];
  1374|
  1375|        db.insert_files(torrent_id, &files).unwrap();
  1376|
  1377|        let dirs = db.get_torrent_directories_by_parent(None, torrent_id).unwrap();
  1378|        let dir1 = dirs.iter().find(|d| d.name == "dir1").unwrap();
  1379|
  1380|        let dir_files = db.get_files_in_directory(dir1.id).unwrap();
  1381|        assert_eq!(dir_files.len(), 2);
  1382|    }
  1383|
  1384|    #[test]
  1385|    fn test_get_all_files_under_directory() {
  1386|        let mut db = Database::open_in_memory().unwrap();
  1387|        
  1388|        let torrent_id = match db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 2).unwrap() {
  1389|            InsertTorrentResult::Inserted(id) => id,
  1390|            _ => panic!("Expected Inserted"),
  1391|        };
  1392|
  1393|        let files = vec![
  1394|            FileEntry { path: "dir1/subdir/file1.txt".to_string(), size: 100 },
  1395|            FileEntry { path: "dir1/file2.txt".to_string(), size: 200 },
  1396|        ];
  1397|
  1398|        db.insert_files(torrent_id, &files).unwrap();
  1399|
  1400|        let dirs = db.get_torrent_directories_by_parent(None, torrent_id).unwrap();
  1401|        let dir1 = dirs.iter().find(|d| d.name == "dir1").unwrap();
  1402|
  1403|        let all_files = db.get_all_files_under_directory(dir1.id).unwrap();
  1404|        assert_eq!(all_files.len(), 2);
  1405|    }
  1406|
  1407|    #[test]
  1408|    fn test_persistence() {
  1409|        let temp_file = NamedTempFile::new().unwrap();
  1410|        let path = temp_file.path();
  1411|
  1412|        {
  1413|            let mut db = Database::open(path).unwrap();
  1414|            db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 1).unwrap();
  1415|        }
  1416|
  1417|        {
  1418|            let db = Database::open(path).unwrap();
  1419|            let torrent = db.get_torrent_by_source_path("path1").unwrap().unwrap();
  1420|            assert_eq!(torrent.name, "Test");
  1421|            assert_eq!(torrent.status, TorrentStatus::Pending);
  1422|        }
  1423|    }
  1424|
  1425|    #[test]
  1426|    fn test_get_torrent_by_info_hash() {
  1427|        let mut db = Database::open_in_memory().unwrap();
  1428|        
  1429|        db.insert_torrent("path1", "Test", "Test", 1024, "abc123", 1).unwrap();
  1430|        
  1431|        let torrent = db.get_torrent_by_info_hash("abc123").unwrap().unwrap();
  1432|        assert_eq!(torrent.source_path, "path1");
  1433|    }
  1434|
  1435|    #[test]
  1436|    fn test_get_all_torrents() {
  1437|        let mut db = Database::open_in_memory().unwrap();
  1438|        
  1439|        db.insert_torrent("path1", "Torrent 1", "Torrent 1", 1024, "hash1", 1).unwrap();
  1440|        db.insert_torrent("path2", "Torrent 2", "Torrent 2", 2048, "hash2", 1).unwrap();
  1441|        
  1442|        let torrents = db.get_all_torrents().unwrap();
  1443|        assert_eq!(torrents.len(), 2);
  1444|    }
  1445|
  1446|    #[test]
  1447|    fn test_nested_directory_structure() {
  1448|        let mut db = Database::open_in_memory().unwrap();
  1449|        
  1450|        let torrent_id = match db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 1).unwrap() {
  1451|            InsertTorrentResult::Inserted(id) => id,
  1452|            _ => panic!("Expected Inserted"),
  1453|        };
  1454|
  1455|        let files = vec![
  1456|            FileEntry { path: "a/b/c/file.txt".to_string(), size: 100 },
  1457|        ];
  1458|
  1459|        db.insert_files(torrent_id, &files).unwrap();
  1460|
  1461|        let all_files = db.get_files_by_torrent_id(torrent_id).unwrap();
  1462|        assert_eq!(all_files.len(), 1);
  1463|    }
  1464|
  1465|    #[test]
  1466|    fn test_get_torrents_by_source_path() {
  1467|        let mut db = Database::open_in_memory().unwrap();
  1468|        
  1469|        db.insert_torrent("path1", "Torrent 1", "Torrent 1", 1024, "hash1", 1).unwrap();
  1470|        db.insert_torrent("path2", "Torrent 2", "Torrent 2", 2048, "hash2", 1).unwrap();
  1471|        db.insert_torrent("other", "Torrent 3", "Torrent 3", 3072, "hash3", 1).unwrap();
  1472|        
  1473|        let torrents = db.get_torrents_by_source_path("path1").unwrap();
  1474|        assert_eq!(torrents.len(), 1);
  1475|        assert_eq!(torrents[0].name, "Torrent 1");
  1476|        
  1477|        let torrents = db.get_torrents_by_source_path("nonexistent").unwrap();
  1478|        assert_eq!(torrents.len(), 0);
  1479|    }
  1480|
  1481|    #[test]
  1482|    fn test_get_source_path_prefixes() {
  1483|        let mut db = Database::open_in_memory().unwrap();
  1484|        
  1485|        db.insert_torrent("a/b", "Torrent 1", "Torrent 1", 1024, "hash1", 1).unwrap();
  1486|        db.insert_torrent("a/c", "Torrent 2", "Torrent 2", 2048, "hash2", 1).unwrap();
  1487|        db.insert_torrent("d", "Torrent 3", "Torrent 3", 3072, "hash3", 1).unwrap();
  1488|        
  1489|        let prefixes = db.get_source_path_prefixes("").unwrap();
  1490|        assert!(prefixes.contains(&"a".to_string()));
  1491|        assert!(prefixes.contains(&"d".to_string()));
  1492|        
  1493|        let prefixes = db.get_source_path_prefixes("a").unwrap();
  1494|        assert!(prefixes.contains(&"b".to_string()));
  1495|        assert!(prefixes.contains(&"c".to_string()));
  1496|    }
  1497|
  1498|    #[test]
  1499|    fn test_metadata_directory_structure_preserved() {
  1500|        let mut db = Database::open_in_memory().unwrap();
  1501|        
  1502|        db.insert_torrent("anime/naruto/season1", "Naruto S1", "Naruto S1", 1024, "hash1", 1).unwrap();
  1503|        db.insert_torrent("anime/naruto/season2", "Naruto S2", "Naruto S2", 2048, "hash2", 1).unwrap();
  1504|        db.insert_torrent("anime/onepiece", "One Piece", "One Piece", 3072, "hash3", 1).unwrap();
  1505|        db.insert_torrent("movies/scifi", "SciFi Movies", "SciFi Movies", 4096, "hash4", 1).unwrap();
  1506|        
  1507|        let root = db.get_source_path_prefixes("").unwrap();
  1508|        assert_eq!(root.len(), 2);
  1509|        assert!(root.contains(&"anime".to_string()));
  1510|        assert!(root.contains(&"movies".to_string()));
  1511|        
  1512|        let anime = db.get_source_path_prefixes("anime").unwrap();
  1513|        assert_eq!(anime.len(), 2);
  1514|        assert!(anime.contains(&"naruto".to_string()));
  1515|        assert!(anime.contains(&"onepiece".to_string()));
  1516|        
  1517|        let naruto = db.get_source_path_prefixes("anime/naruto").unwrap();
  1518|        assert_eq!(naruto.len(), 2);
  1519|        assert!(naruto.contains(&"season1".to_string()));
  1520|        assert!(naruto.contains(&"season2".to_string()));
  1521|        
  1522|        let onepiece = db.get_source_path_prefixes("anime/onepiece").unwrap();
  1523|        assert_eq!(onepiece.len(), 0);
  1524|        
  1525|        let movies = db.get_source_path_prefixes("movies").unwrap();
  1526|        assert_eq!(movies.len(), 1);
  1527|        assert!(movies.contains(&"scifi".to_string()));
  1528|    }
  1529|
  1530|    #[test]
  1531|    fn test_get_root_files() {
  1532|        let mut db = Database::open_in_memory().unwrap();
  1533|        
  1534|        let torrent_id = match db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 3).unwrap() {
  1535|            InsertTorrentResult::Inserted(id) => id,
  1536|            _ => panic!("Expected Inserted"),
  1537|        };
  1538|
  1539|        let files = vec![
  1540|            FileEntry { path: "file1.txt".to_string(), size: 100 },
  1541|            FileEntry { path: "file2.txt".to_string(), size: 200 },
  1542|            FileEntry { path: "dir/file3.txt".to_string(), size: 300 },
  1543|        ];
  1544|
  1545|        db.insert_files(torrent_id, &files).unwrap();
  1546|
  1547|        let root_files = db.get_root_files(torrent_id).unwrap();
  1548|        assert_eq!(root_files.len(), 2);
  1549|    }
  1550|
  1551|    #[test]
  1552|    fn test_get_torrent_directory() {
  1553|        let mut db = Database::open_in_memory().unwrap();
  1554|        
  1555|        let torrent_id = match db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 1).unwrap() {
  1556|            InsertTorrentResult::Inserted(id) => id,
  1557|            _ => panic!("Expected Inserted"),
  1558|        };
  1559|
  1560|        let files = vec![
  1561|            FileEntry { path: "dir1/file.txt".to_string(), size: 100 },
  1562|        ];
  1563|
  1564|        db.insert_files(torrent_id, &files).unwrap();
  1565|
  1566|        let dir = db.get_torrent_directory(torrent_id, None, "dir1").unwrap();
  1567|        assert!(dir.is_some());
  1568|        assert_eq!(dir.unwrap().name, "dir1");
  1569|    }
  1570|
  1571|    #[test]
  1573|    fn test_insert_torrent_with_files_atomic() {
  1574|        let mut db = Database::open_in_memory().unwrap();
  1575|        
  1576|        let files = vec![
  1577|            FileEntry { path: "dir1/file1.txt".to_string(), size: 100 },
  1578|            FileEntry { path: "dir1/file2.txt".to_string(), size: 200 },
  1579|            FileEntry { path: "dir2/file3.txt".to_string(), size: 300 },
  1580|        ];
  1581|
  1582|        // Insert torrent with files atomically
  1583|        let result = db.insert_torrent_with_files(
  1584|            "path1",
  1585|            "Test Torrent",
  1586|            600,
  1587|            "hash1",
  1588|            3,
  1589|            &files,
  1590|        ).unwrap();
  1591|        
  1592|        let torrent_id = match result {
  1593|            InsertTorrentResult::Inserted(id) => id,
  1594|            _ => panic!("Expected Inserted"),
  1595|        };
  1596|
  1597|        // Verify torrent was inserted
  1598|        let torrent = db.get_torrent_by_source_path("path1").unwrap().unwrap();
  1599|        assert_eq!(torrent.name, "Test Torrent");
  1600|        assert_eq!(torrent.total_size, 600);
  1601|
  1602|        // Verify files were inserted
  1603|        let db_files = db.get_files_by_torrent_id(torrent_id).unwrap();
  1604|        assert_eq!(db_files.len(), 3);
  1605|
  1606|        // Verify directories were created
  1607|        let root_dirs = db.get_torrent_directories_by_parent(None, torrent_id).unwrap();
  1608|        assert_eq!(root_dirs.len(), 2);
  1609|    }
  1610|
  1611|    #[test]
  1612|    fn test_insert_torrent_with_files_duplicate() {
  1613|        let mut db = Database::open_in_memory().unwrap();
  1614|        
  1615|        let files = vec![
  1616|            FileEntry { path: "file1.txt".to_string(), size: 100 },
  1617|        ];
  1618|
  1619|        // First insert
  1620|        let result = db.insert_torrent_with_files(
  1621|            "path1",
  1622|            "Test Torrent",
  1623|            100,
  1624|            "hash1",
  1625|            1,
  1626|            &files,
  1627|        ).unwrap();
  1628|        assert!(matches!(result, InsertTorrentResult::Inserted(_)));
  1629|
  1630|        // Second insert with same info_hash and source_path should return Duplicate
  1631|        let result = db.insert_torrent_with_files(
  1632|            "path1",
  1633|            "Test Torrent 2",
  1634|            200,
  1635|            "hash1",
  1636|            1,
  1637|            &files,
  1638|        ).unwrap();
  1639|        assert!(matches!(result, InsertTorrentResult::Duplicate(_)));
  1641|    fn test_get_all_metadata_dirs_ordered() {
  1642|        let mut db = Database::open_in_memory().unwrap();
  1643|        
  1644|        // Create nested directory structure: a/b/c and a/b/d
  1645|        db.insert_torrent("a/b/c", "Torrent 1", 1024, "hash1", 1).unwrap();
  1646|        db.insert_torrent("a/b/d", "Torrent 2", 2048, "hash2", 1).unwrap();
  1647|        db.insert_torrent("x/y", "Torrent 3", 3072, "hash3", 1).unwrap();
  1648|        
  1649|        let dirs = db.get_all_metadata_dirs_ordered().unwrap();
  1650|        
  1651|        // Should have 5 directories: a, a/b, a/b/c, a/b/d, x, x/y
  1652|        // But actually: a, b (under a), c (under a/b), d (under a/b), x, y (under x)
  1653|        // That's 6 directories
  1654|        assert_eq!(dirs.len(), 6);
  1655|        
  1656|        // Build a map of path -> (index in result, parent_id)
  1657|        let mut path_positions: std::collections::HashMap<String, (usize, Option<i64>)> = 
  1658|            std::collections::HashMap::new();
  1659|        for (idx, (_id, parent_id, _name, path)) in dirs.iter().enumerate() {
  1660|            path_positions.insert(path.clone(), (idx, *parent_id));
  1661|        }
  1662|        
  1663|        // Verify that "a" comes before "a/b"
  1664|        let a_pos = path_positions.get("a").map(|(pos, _)| *pos);
  1665|        let ab_pos = path_positions.get("a/b").map(|(pos, _)| *pos);
  1666|        assert!(a_pos.is_some(), "a should exist");
  1667|        assert!(ab_pos.is_some(), "a/b should exist");
  1668|        assert!(a_pos < ab_pos, "a should come before a/b: {:?} vs {:?}", a_pos, ab_pos);
  1669|        
  1670|        // Verify that "a/b" comes before "a/b/c"
  1671|        let abc_pos = path_positions.get("a/b/c").map(|(pos, _)| *pos);
  1672|        assert!(abc_pos.is_some(), "a/b/c should exist");
  1673|        assert!(ab_pos < abc_pos, "a/b should come before a/b/c: {:?} vs {:?}", ab_pos, abc_pos);
  1674|        
  1675|        // Verify that "a/b" comes before "a/b/d"
  1676|        let abd_pos = path_positions.get("a/b/d").map(|(pos, _)| *pos);
  1677|        assert!(abd_pos.is_some(), "a/b/d should exist");
  1678|        assert!(ab_pos < abd_pos, "a/b should come before a/b/d: {:?} vs {:?}", ab_pos, abd_pos);
  1679|        
  1680|        // Verify that "x" comes before "x/y"
  1681|        let x_pos = path_positions.get("x").map(|(pos, _)| *pos);
  1682|        let xy_pos = path_positions.get("x/y").map(|(pos, _)| *pos);
  1683|        assert!(x_pos.is_some(), "x should exist");
  1684|        assert!(xy_pos.is_some(), "x/y should exist");
  1685|        assert!(x_pos < xy_pos, "x should come before x/y: {:?} vs {:?}", x_pos, xy_pos);
  1687|    }
  1688|}
  1689|
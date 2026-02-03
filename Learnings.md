# Data Engineering Learnings

A collection of problems encountered, solutions applied, and lessons learned during the data engineering journey.

---

## Docker Compose YAML Indentation Error

### The Problem

When running `docker-compose config` or trying to start services, you might encounter this error:

```
yaml: line 3: did not find expected key
```

This is one of the most common errors when working with Docker Compose files, but the error message can be misleading - it doesn't always point to the exact line with the problem.

### Understanding YAML Syntax

**YAML (YAML Ain't Markup Language)** is a human-readable data serialization format that relies heavily on **indentation** to define structure. Unlike JSON which uses braces `{}` and brackets `[]`, YAML uses whitespace indentation to create hierarchy.

#### Key YAML Rules:

1. **Indentation must be consistent** - Use either 2 or 4 spaces (never tabs!)
2. **Spaces matter** - Each level of nesting adds the same number of spaces
3. **Colons** - Key-value pairs use a colon: `key: value`
4. **Lists** - Use dashes: `- item`
5. **Nested structures** - Child elements must be indented more than their parent

#### Example of Correct Docker Compose Structure:

```yaml
services:                    # Top-level key (0 spaces)
  service_name:              # Service name (2 spaces)
    image: postgres:18       # Service property (4 spaces)
    environment:             # Another property (4 spaces)
      VAR_NAME: value        # Nested value (6 spaces)
    volumes:                 # List property (4 spaces)
      - volume_name:/path    # List item (6 spaces)
  another_service:           # Another service (2 spaces, same level as first)
    image: nginx:latest      # Property (4 spaces)
```

### The Actual Problem in Our Case

The error said "line 3" but the real issue was on lines 36-51. Here's what was wrong:

```yaml
# INCORRECT ❌
services:
  pgdatabase:
    image: postgres:18
  
  kestra_postgres:          # Should be at 2 spaces
      image: postgres:18    # Properties at 6 spaces instead of 4!
      volumes:
        - data:/path
      
    kestra:                 # Should be at 2 spaces (same as kestra_postgres)
      image: kestra:v1.1    # Should be at 4 spaces
      ports:                # Should be at 4 spaces
        - "8080:8080"
```

The indentation was inconsistent:
- `kestra_postgres` service properties were indented 6 spaces instead of 4
- `kestra` service was indented 4 spaces instead of 2 (not aligned with other service names)
- `kestra` service properties were indented 6 spaces instead of 4

### Why Did the Error Say "Line 3"?

YAML parsers often report errors at the point where they **realize** something is wrong, not where the actual mistake is. When it hit the incorrectly indented `kestra:` service, it was still parsing the `kestra_postgres:` service block and got confused, backtracking to report an error near where it started parsing services.

### How to Troubleshoot YAML Indentation Issues

#### Step 1: Validate the YAML Syntax

```bash
docker-compose config
```

This command parses your `docker-compose.yaml` file and shows you if there are syntax errors.

#### Step 2: Use Python's YAML Parser for Better Error Messages

```bash
python3 -c "import yaml; yaml.safe_load(open('docker-compose.yaml'))"
```

Python's YAML parser often gives more detailed error messages showing both where it started parsing and where it found the problem:

```
yaml.parser.ParserError: while parsing a block mapping
  in "docker-compose.yaml", line 4, column 3
expected <block end>, but found '<block mapping start>'
  in "docker-compose.yaml", line 51, column 5
```

This tells you that while parsing something starting at line 4, it encountered an unexpected block at line 51!

#### Step 3: Visualize Indentation

Use `cat -A` to see all characters including spaces:

```bash
cat -A docker-compose.yaml | head -20
```

The `$` symbols show line endings, helping you spot inconsistent spacing.

#### Step 4: Check Indentation Systematically

Work through your file checking that:
- All service names are at the **same indentation level** (typically 2 spaces under `services:`)
- All properties of a service are at the **same indentation level** (typically 2 spaces under the service name)
- Nested values are consistently indented

### The Solution

Fix all indentation to be consistent:

```yaml
# CORRECT ✅
services:
  pgdatabase:              # 2 spaces
    image: postgres:18     # 4 spaces (2 more than parent)
    
  kestra_postgres:         # 2 spaces (aligned with other services)
    image: postgres:18     # 4 spaces
    volumes:               # 4 spaces
      - data:/path         # 6 spaces
    environment:           # 4 spaces
      VAR: value           # 6 spaces
    networks:              # 4 spaces
      - pg-network         # 6 spaces
      
  kestra:                  # 2 spaces (aligned with other services)
    image: kestra:v1.1     # 4 spaces
    ports:                 # 4 spaces
      - "8080:8080"        # 6 spaces
    depends_on:            # 4 spaces
      kestra_postgres:     # 6 spaces
        condition: healthy # 8 spaces
```

### Best Practices to Avoid YAML Issues

1. **Use a good editor** - VS Code, Vim, or any editor with YAML syntax highlighting
2. **Enable "Show Whitespace"** in your editor settings
3. **Use a YAML linter** - Install YAML extensions that catch errors as you type
4. **Be consistent** - Choose 2-space or 4-space indentation and stick with it
5. **Never mix tabs and spaces** - Configure your editor to insert spaces when you press Tab
6. **Validate early, validate often** - Run `docker-compose config` after making changes
7. **Use online YAML validators** - Tools like yamllint.com can help visualize structure

### Key Takeaways

- YAML errors can be cryptic - the reported line number isn't always where the problem is
- Indentation is **semantic** in YAML - it defines the data structure, not just aesthetics
- All sibling elements (elements at the same level) must have identical indentation
- Use multiple validation tools to triangulate the actual problem location
- Understanding how YAML parsers work helps you debug issues faster

### Related Concepts

- **Data Serialization**: Converting data structures into a format that can be stored or transmitted
- **Markup Languages**: Systems for annotating text to define structure (XML, HTML, YAML, TOML)
- **Parser**: A program that reads text and builds a data structure from it
- **Syntax Trees**: The hierarchical structure that parsers create from nested data formats

---

## Docker Compose External Network Error

### The Problem

After fixing the YAML syntax and running `docker-compose up`, you might encounter:

```
network pg-network declared as external, but could not be found
```

All your images pull successfully, but the containers fail to start because they can't find the network they're supposed to join.

### Understanding Docker Networks

**Docker networks** allow containers to communicate with each other. Docker Compose can either:
1. **Create and manage networks automatically** (default behavior)
2. **Use existing networks** that were created manually (external networks)

#### Types of Docker Networks:

- **Bridge** (default): Containers on the same bridge network can communicate
- **Host**: Container uses the host's network directly (no isolation)
- **None**: Container has no network access
- **Custom bridge**: User-defined networks with better DNS resolution

#### Why Use External Networks?

External networks are useful when:
- You have **multiple Docker Compose projects** that need to communicate
- You want to **share a network** between manually-started containers and Compose services
- You need the network to **persist** even when you run `docker-compose down`
- You're integrating with **existing infrastructure**

### The Actual Problem in Our Case

In our [docker-compose.yaml](docker-compose.yaml), we had:

```yaml
networks:
  pg-network:
    external: true  # <-- Telling Docker: "This network already exists!"
```

When you set `external: true`, Docker Compose expects that network to **already exist**. It won't create it for you. Since we never created `pg-network` manually, Docker couldn't find it.

### How to Troubleshoot Docker Network Issues

#### Step 1: List All Docker Networks

```bash
docker network ls
```

This shows all networks currently available. Look for your network name in the list.

Output example:
```
NETWORK ID     NAME              DRIVER    SCOPE
abc123def456   bridge            bridge    local
xyz789uvw012   host              host      local
def456ghi789   none              null      local
```

If `pg-network` isn't in this list and you declared it as `external: true`, that's your problem!

#### Step 2: Inspect a Specific Network (if it exists)

```bash
docker network inspect pg-network
```

This shows detailed information about the network including which containers are connected to it.

#### Step 3: Check Docker Compose Configuration

```bash
docker-compose config
```

This validates your YAML and shows how Docker Compose interprets your network configuration.

### Solutions

#### Option 1: Create the External Network Manually

If you **want** an external network (to share between multiple projects), create it first:

```bash
# Create the network
docker network create pg-network

# Verify it was created
docker network ls | grep pg-network

# Now start your services
docker-compose up
```

The network will persist even after `docker-compose down`, so other projects can use it too.

#### Option 2: Let Docker Compose Manage the Network (Recommended for Single Projects)

If you don't need to share the network with other projects, remove the `external: true` declaration:

```yaml
# BEFORE (requires manual network creation) ❌
networks:
  pg-network:
    external: true

# AFTER (Docker Compose creates it automatically) ✅
networks:
  pg-network:
    driver: bridge
```

Or even simpler - just declare the network name:

```yaml
networks:
  pg-network:
```

Docker Compose will:
- Create the network when you run `docker-compose up`
- Name it `<project>_pg-network` (e.g., `pipeline_pg-network`)
- Remove it when you run `docker-compose down`

#### Option 3: Use the Default Network

If you don't need custom network configuration at all, you can remove the `networks:` section entirely. Docker Compose creates a default network for your project automatically.

### Comparing Network Strategies

| Strategy | Network Name | Lifecycle | Use Case |
|----------|-------------|-----------|----------|
| **External** | Exact name you specify | Persists independently | Multiple projects sharing network |
| **Managed** | `<project>_<network-name>` | Created/destroyed with compose | Single project, isolated |
| **Default** | `<project>_default` | Created/destroyed with compose | Simple projects, no custom config needed |

### Real-World Example

**Scenario**: You have two Docker Compose projects that need to communicate:
- Project A: PostgreSQL database
- Project B: Web application that connects to the database

**Solution**: Use an external network

```bash
# Create shared network once
docker network create shared-network

# Project A (docker-compose.yaml)
services:
  postgres:
    image: postgres:18
    networks:
      - shared-network

networks:
  shared-network:
    external: true

# Project B (docker-compose.yaml)
services:
  webapp:
    image: myapp:latest
    networks:
      - shared-network

networks:
  shared-network:
    external: true
```

Now both projects can communicate through `shared-network`!

### Best Practices for Docker Networks

1. **Default for simple projects** - Don't create networks unless you need them
2. **External for multi-project setups** - Use when containers need to communicate across compose files
3. **Document your networks** - Add comments explaining why a network is external
4. **Clean up unused networks** - Run `docker network prune` periodically
5. **Use descriptive names** - `pg-network` is better than `net1`
6. **Check network existence** - Before using `external: true`, verify the network exists
7. **Consider network isolation** - Different networks provide security boundaries

### Troubleshooting Commands

```bash
# List all networks
docker network ls

# Create a network
docker network create my-network

# Inspect a network (see connected containers)
docker network inspect my-network

# Remove a network
docker network rm my-network

# Remove all unused networks
docker network prune

# Connect a running container to a network
docker network connect my-network container-name

# Disconnect a container from a network
docker network disconnect my-network container-name
```

### Additional Notes

**Warning about GEMINI_API_KEY**: You might also see:
```
WARN[0000] The "GEMINI_API_KEY" variable is not set. Defaulting to a blank string.
```

This is a **warning**, not an error. The containers will still start, but Kestra's AI Copilot feature won't work without the API key. To fix this:

```bash
# Temporarily (current session only)
export GEMINI_API_KEY="your-actual-api-key"
docker-compose up

# Permanently (add to ~/.bashrc or ~/.zshrc)
echo 'export GEMINI_API_KEY="your-actual-api-key"' >> ~/.bashrc
source ~/.bashrc

# Or use a .env file in your project directory
echo 'GEMINI_API_KEY=your-actual-api-key' > .env
docker-compose up
```

Docker Compose automatically loads variables from a `.env` file in the same directory as your `docker-compose.yaml`.

### Key Takeaways

- **External networks must be created manually** before running `docker-compose up`
- The error message is clear but you need to understand what "external" means
- Choose the right network strategy for your use case (external vs managed vs default)
- Docker networks enable container communication and provide isolation
- Always verify network existence with `docker network ls` before declaring it external
- Clean documentation prevents confusion when revisiting projects months later

### Related Concepts

- **Container Orchestration**: Managing multiple containers and their interactions
- **Service Discovery**: How containers find and connect to each other
- **Network Isolation**: Using separate networks as security boundaries
- **Bridge Networking**: The default Docker networking driver that creates virtual networks
- **DNS Resolution**: Docker's built-in DNS for container name resolution within networks

---

## wget Pipe to gunzip Not Working

### The Problem

When trying to download and decompress a gzipped file in one command, you might run:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz | gunzip > green_tripdata_2019-01.csv
```

But you get the error:

```
gzip: stdin: unexpected end of file
```

And instead of your decompressed CSV, wget creates a file like `green_tripdata_2019-01.csv.gz.1` (with a `.1` suffix indicating a duplicate filename).

### Understanding Unix Pipes and Standard Streams

In Unix/Linux, every program has three standard streams:

1. **stdin (standard input)** - Where a program reads input from (file descriptor 0)
2. **stdout (standard output)** - Where a program writes normal output (file descriptor 1)  
3. **stderr (standard error)** - Where a program writes error messages (file descriptor 2)

#### How Pipes Work:

The pipe symbol `|` connects the **stdout** of one command to the **stdin** of the next:

```
Command1 stdout ──|──> Command2 stdin
```

For example:
```bash
cat file.txt | grep "hello" | wc -l
```

This chain works because:
- `cat` outputs the file contents to stdout
- `grep` reads from stdin, filters, outputs matches to stdout
- `wc -l` reads from stdin, counts lines, outputs the count

### The Actual Problem

**wget's default behavior** is NOT to send downloaded content to stdout. Instead, it:
1. Saves the file to disk with its original filename
2. Sends **progress messages and status information** to stdout (actually stderr, but visible in terminal)

So when you run:
```bash
wget https://example.com/file.gz | gunzip > output.csv
```

Here's what actually happens:
1. wget downloads `file.gz` and **saves it to disk** as `file.gz`
2. wget's status messages (progress bar, connection info) go to the terminal
3. The pipe `|` sends those text status messages to gunzip
4. gunzip tries to decompress text like "Connecting to github.com..." as if it were gzip data
5. gunzip fails with "unexpected end of file" because the text isn't valid gzip format
6. Your output file is empty or contains garbage

### How to Diagnose This

#### Check What wget Actually Does:

```bash
# Run wget and observe - notice it creates a file on disk
wget https://example.com/somefile.gz

# List files to see what was created
ls -la
```

You'll see a new file on disk, confirming wget saved to a file rather than sending to stdout.

#### Check What's Going Through the Pipe:

```bash
# See what wget sends to stdout (spoiler: not the file content!)
wget https://example.com/somefile.gz 2>&1 | head -5
```

You'll see text like:
```
--2026-02-02 17:23:58--  https://example.com/somefile.gz
Resolving example.com...
Connecting to example.com...
HTTP request sent, awaiting response... 200 OK
```

That's what gunzip was trying to decompress!

### The Solution

Tell wget to output the downloaded content to stdout using the `-O -` flag:

```bash
# The -O flag specifies output destination
# The - (dash) means "stdout" in Unix convention
wget -O - https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz | gunzip > green_tripdata_2019-01.csv
```

Or use `-qO-` to also suppress progress messages:
- `-q` = quiet mode (no progress output)
- `-O -` = output to stdout

```bash
wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz | gunzip > green_tripdata_2019-01.csv
```

### Alternative Solutions

#### Option 1: Use curl Instead

`curl` outputs to stdout by default, which is often more intuitive for piping:

```bash
curl -sL https://example.com/file.gz | gunzip > output.csv
```

Flags:
- `-s` = silent (no progress bar)
- `-L` = follow redirects (important for GitHub releases!)

#### Option 2: Download Then Decompress (Two Steps)

```bash
# Step 1: Download the file
wget https://example.com/file.gz

# Step 2: Decompress it
gunzip file.gz
# This creates 'file' and removes 'file.gz'

# Or keep the original:
gunzip -c file.gz > file
# Or:
gunzip -k file.gz
```

#### Option 3: Use zcat

`zcat` is equivalent to `gunzip -c` (decompress to stdout):

```bash
wget https://example.com/file.gz
zcat file.gz > output.csv
```

### Comparing wget and curl

| Feature | wget | curl |
|---------|------|------|
| Default output | Saves to file | Outputs to stdout |
| Output to stdout | `-O -` | Default (or `-o -`) |
| Follow redirects | Default | Requires `-L` |
| Resume downloads | `-c` | `-C -` |
| Recursive download | Yes (`-r`) | No |
| Progress display | Default on | Default on (use `-s` to hide) |

### The Dash (-) Convention in Unix

In many Unix commands, a single dash `-` represents stdin or stdout depending on context:

```bash
# Read from stdin
cat -                    # cat reads from keyboard/stdin
echo "hello" | cat -     # cat reads from pipe

# Write to stdout  
wget -O - url            # wget outputs to stdout
tar -xzf - < file.tar.gz # tar reads from stdin

# Some commands use it for both
cat file.txt | gzip - > file.gz    # gzip reads stdin, writes stdout
```

This convention allows you to build powerful pipelines by connecting commands together.

### Common wget Flags

```bash
# Output to stdout (for piping)
wget -O - url

# Quiet mode (no progress)
wget -q url

# Combine: quiet + stdout
wget -qO- url

# Save with different filename
wget -O myfile.csv url

# Continue interrupted download
wget -c url

# Download in background
wget -b url

# Limit download speed
wget --limit-rate=1m url

# Set timeout
wget -T 30 url

# User agent string
wget -U "Mozilla/5.0" url
```

### Real-World Pipeline Example

Here's a complete pipeline for downloading, decompressing, processing, and loading data:

```bash
# Download gzipped CSV, decompress, remove trailing commas, take first 1000 lines
wget -qO- https://example.com/data.csv.gz \
  | gunzip \
  | sed 's/,$//' \
  | head -n 1000 \
  > sample_data.csv
```

Each step:
1. `wget -qO-` - Download and output to stdout
2. `gunzip` - Decompress the stream
3. `sed 's/,$//'` - Remove trailing commas
4. `head -n 1000` - Take only first 1000 lines
5. `> sample_data.csv` - Save to file

### Key Takeaways

- **wget saves to files by default** - it doesn't output to stdout like many Unix tools
- Use **`-O -`** to make wget output to stdout for piping
- The **dash `-` convention** means stdin/stdout in many Unix commands
- **curl outputs to stdout by default** - often easier for simple piping scenarios
- Always **understand what each command outputs** before building pipelines
- **"Unexpected end of file"** from gzip/gunzip usually means you're feeding it non-gzip data
- When debugging pipes, **test each step individually** to see what's actually flowing through

### Related Concepts

- **Unix Philosophy**: Small programs that do one thing well, connected via pipes
- **Standard Streams**: stdin, stdout, stderr - the three default I/O channels
- **File Descriptors**: Numeric handles for I/O (0=stdin, 1=stdout, 2=stderr)
- **Redirection**: Using `>`, `>>`, `<`, `2>&1` to control where data flows
- **Process Substitution**: Using `<(command)` to treat command output as a file

---

## CSV Trailing Comma Causing Column Mismatch

### The Problem

When loading a CSV file into a database using Kestra's `CopyIn` task (or any bulk loader), you get an error like:

```
ERROR: extra data after last expected column
```

Or:
```
Column count mismatch: expected 19 columns but found 20
```

The CSV looks fine when you open it, but the database keeps complaining about an extra column.

### Understanding CSV Format

**CSV (Comma-Separated Values)** is a simple text format where:
- Each line is a record (row)
- Fields (columns) are separated by commas
- The number of commas determines the number of fields

#### How CSV Parsers Count Columns:

```csv
a,b,c      → 3 columns: ["a", "b", "c"]
a,b,c,     → 4 columns: ["a", "b", "c", ""]  ← trailing comma = empty 4th column!
a,b,       → 3 columns: ["a", "b", ""]
,b,c       → 3 columns: ["", "b", "c"]
```

**Key insight**: A trailing comma creates an additional empty column!

### The Actual Problem

The NYC taxi dataset from GitHub has **trailing commas** after every row:

```csv
VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,...,congestion_surcharge
2,2018-12-21 15:17:29,2018-12-21 15:18:57,...,1,    ← Notice trailing comma!
2,2019-01-01 00:10:16,2019-01-01 00:16:32,...,1,    ← Every row has one!
```

This means:
- Header declares 20 columns
- Each data row appears to have 21 values (20 real + 1 empty)
- Database expects 20 columns but receives 21

When Kestra's `CopyIn` tries to load this into PostgreSQL:
1. PostgreSQL counts 21 fields in each row
2. The table only has 20 columns
3. PostgreSQL throws an error about the mismatch

### How to Diagnose This

#### Step 1: Look at the Raw Data

```bash
# View first few lines of the CSV
head -n 3 data.csv

# Or download and peek directly
wget -qO- https://example.com/data.csv.gz | gunzip | head -n 3
```

Look carefully at the end of each line for trailing commas.

#### Step 2: Count Columns in Header vs Data

```bash
# Count commas in first line (header)
head -n 1 data.csv | tr -cd ',' | wc -c

# Count commas in second line (first data row)
sed -n '2p' data.csv | tr -cd ',' | wc -c
```

If the data row has one more comma than the header, you have trailing commas.

#### Step 3: Use awk to Count Fields

```bash
# Show field count for first 5 rows
head -n 5 data.csv | awk -F',' '{print NF}'
```

Output might show:
```
20    ← header (no trailing comma, or trailing comma counts as empty field)
21    ← data rows have one extra
21
21
21
```

### The Solution

Remove trailing commas before loading the data. The tool `sed` (stream editor) is perfect for this:

```bash
# sed 's/,$//' means:
#   s     = substitute
#   ,$    = comma at end of line ($ = end anchor)
#   //    = replace with nothing (delete it)

# Apply to a file
sed 's/,$//' input.csv > cleaned.csv

# Or in a pipeline
wget -qO- https://example.com/data.csv.gz | gunzip | sed 's/,$//' > clean.csv
```

#### In Kestra Flow:

```yaml
- id: extract
  type: io.kestra.plugin.scripts.shell.Commands
  outputFiles:
    - "*.csv"
  taskRunner:
    type: io.kestra.plugin.core.runner.Process
  commands:
    - wget -qO- https://example.com/data.csv.gz | gunzip | sed 's/,$//' > data.csv
```

### Alternative Solutions

#### Option 1: Use awk Instead of sed

```bash
# Remove last field if empty
awk -F',' 'BEGIN{OFS=","} {if($NF=="") NF--; print}' input.csv > cleaned.csv
```

#### Option 2: Use Perl

```bash
perl -pe 's/,$//' input.csv > cleaned.csv
```

#### Option 3: Handle in Python

```python
import pandas as pd

# pandas often handles this automatically
df = pd.read_csv('data.csv')

# Or explicitly handle trailing columns
df = pd.read_csv('data.csv', usecols=range(20))  # Only read first 20 columns
```

#### Option 4: Specify Columns in Database Import

Some tools let you specify exactly which columns to import, ignoring extras:

```sql
-- PostgreSQL COPY with explicit column list
COPY table_name (col1, col2, col3, ..., col20) 
FROM '/path/to/data.csv' 
WITH (FORMAT CSV, HEADER TRUE);
```

### Understanding sed Regular Expressions

```bash
sed 's/pattern/replacement/'
```

Common patterns:
```bash
s/,$/        # Comma at end of line
s/^,//       # Comma at start of line
s/,,/,/g     # Double comma → single comma (g = global, all occurrences)
s/[[:space:]]*$//  # Trailing whitespace
s/^[[:space:]]*//  # Leading whitespace
s/"//g       # Remove all quotes
```

The `$` anchor is crucial - it means "end of line", so `,$ ` matches only a comma that's the last character.

### Why Do CSV Files Have Trailing Commas?

This often happens when:
1. **Data export tools add them** - Some poorly designed export processes add a trailing delimiter
2. **Empty last column** - The last column is always empty, and the exporter faithfully includes the comma
3. **Spreadsheet software quirks** - Excel and other tools can create this when saving as CSV
4. **Data concatenation** - When files are joined, formatting inconsistencies can appear
5. **Schema changes** - A column was removed but the comma wasn't

### Best Practices for CSV Handling

1. **Always inspect raw data** before trying to load it
   ```bash
   head -n 5 data.csv
   tail -n 5 data.csv
   ```

2. **Count your columns** to verify structure
   ```bash
   head -n 1 data.csv | awk -F',' '{print NF}'
   ```

3. **Check for common issues**:
   - Trailing commas
   - Inconsistent quoting
   - Embedded newlines
   - Mixed delimiters (commas vs semicolons)

4. **Clean data in the pipeline** rather than modifying source files:
   ```bash
   wget ... | gunzip | sed 's/,$//' | ...
   ```

5. **Document known data quirks** for future reference

6. **Validate after loading**:
   ```sql
   SELECT COUNT(*) FROM table;
   SELECT * FROM table LIMIT 5;
   ```

### Key Takeaways

- **Trailing commas create empty columns** in CSV parsing
- A file can "look fine" but have hidden formatting issues
- **sed 's/,$//'** is the quick fix for trailing commas
- Always **inspect your raw data** before loading
- **Clean data in the pipeline** using stream processing tools
- Database error messages about "extra columns" often indicate CSV formatting issues
- CSV is **deceptively simple** - many edge cases can cause problems

### Related Concepts

- **Data Validation**: Checking data quality before loading
- **ETL (Extract, Transform, Load)**: Cleaning happens in the Transform step
- **Stream Processing**: Processing data as it flows through, rather than loading into memory
- **Regular Expressions**: Pattern matching for text manipulation
- **Data Serialization Formats**: CSV, JSON, Parquet, and their trade-offs

---

## Green vs Yellow Taxi Data Column Names

### The Problem

When setting up a data pipeline for NYC taxi data, you might create a database table and import task with column names like:

```yaml
columns:
  - VendorID
  - tpep_pickup_datetime    # ← Looks reasonable, right?
  - tpep_dropoff_datetime
  - passenger_count
  ...
```

But when loading **green taxi** data, you get errors about column mismatches or null values where you expected timestamps.

### Understanding the NYC Taxi Dataset

The NYC Taxi & Limousine Commission (TLC) releases trip data for different taxi types:

| Taxi Type | Color | Description |
|-----------|-------|-------------|
| **Yellow** | Yellow | Traditional NYC yellow cabs, primarily Manhattan |
| **Green** | Green | Boro taxis, serving outer boroughs (introduced 2013) |
| **FHV** | Various | For-Hire Vehicles (Uber, Lyft, etc.) |
| **HVFHV** | Various | High-Volume For-Hire Vehicles |

**Each taxi type has a different CSV schema!**

### The Actual Problem

Yellow and green taxi data have similar but **different column names**:

#### Yellow Taxi Columns:
```csv
VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,...
         ↑                    ↑
      "tpep" = Trip Payment Electronic Processor (Yellow)
```

#### Green Taxi Columns:
```csv
VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,...
         ↑                    ↑
      "lpep" = Local Payment Electronic Processor (Green)
```

Also, **the column order and available columns differ**:

| Column | Yellow Taxi | Green Taxi |
|--------|-------------|------------|
| Pickup datetime | `tpep_pickup_datetime` | `lpep_pickup_datetime` |
| Dropoff datetime | `tpep_dropoff_datetime` | `lpep_dropoff_datetime` |
| E-hail fee | ❌ Not present | ✅ `ehail_fee` |
| Trip type | ❌ Not present | ✅ `trip_type` |
| Store and forward | After passenger_count | After dropoff datetime |

### Green Taxi Complete Schema

Here's the actual column order for green taxi data (as of 2019-2020):

```csv
VendorID
lpep_pickup_datetime
lpep_dropoff_datetime
store_and_fwd_flag
RatecodeID
PULocationID
DOLocationID
passenger_count
trip_distance
fare_amount
extra
mta_tax
tip_amount
tolls_amount
ehail_fee
improvement_surcharge
total_amount
payment_type
trip_type
congestion_surcharge
```

That's **20 columns** (plus a trailing comma making it appear as 21).

### Yellow Taxi Complete Schema

For comparison, yellow taxi data:

```csv
VendorID
tpep_pickup_datetime
tpep_dropoff_datetime
passenger_count
trip_distance
RatecodeID
store_and_fwd_flag
PULocationID
DOLocationID
payment_type
fare_amount
extra
mta_tax
tip_amount
tolls_amount
improvement_surcharge
total_amount
congestion_surcharge
```

That's **18-19 columns** depending on the year.

### How to Diagnose Schema Issues

#### Step 1: Look at the Actual Header

```bash
# Download and check the header
wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz \
  | gunzip \
  | head -n 1
```

This shows you exactly what columns are in the file.

#### Step 2: Compare with Your Table Definition

Your table must match the CSV's:
1. **Column names** (or use column mapping)
2. **Column order** (when using positional loading)
3. **Column count** (same number of columns)

#### Step 3: Check Official Documentation

The NYC TLC provides data dictionaries:
- [Yellow Taxi Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- [Green Taxi Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf)

### The Solution

Match your table and import columns to the actual CSV schema:

```yaml
# For GREEN taxi data
- id: green_create_table
  type: io.kestra.plugin.jdbc.postgresql.Queries
  sql: |
    CREATE TABLE IF NOT EXISTS green_tripdata (
        VendorID               text,
        lpep_pickup_datetime   timestamp,   -- Note: lpep, not tpep!
        lpep_dropoff_datetime  timestamp,
        store_and_fwd_flag     text,
        RatecodeID             text,
        PULocationID           text,
        DOLocationID           text,
        passenger_count        integer,
        trip_distance          double precision,
        fare_amount            double precision,
        extra                  double precision,
        mta_tax                double precision,
        tip_amount             double precision,
        tolls_amount           double precision,
        ehail_fee              double precision,  -- Green taxi only!
        improvement_surcharge  double precision,
        total_amount           double precision,
        payment_type           integer,
        trip_type              integer,           -- Green taxi only!
        congestion_surcharge   double precision
    );

- id: green_copy_in
  type: io.kestra.plugin.jdbc.postgresql.CopyIn
  format: CSV
  from: "{{render(vars.data)}}"
  table: green_tripdata
  header: true
  columns:
    - VendorID
    - lpep_pickup_datetime    # Must match CSV header exactly!
    - lpep_dropoff_datetime
    - store_and_fwd_flag
    - RatecodeID
    - PULocationID
    - DOLocationID
    - passenger_count
    - trip_distance
    - fare_amount
    - extra
    - mta_tax
    - tip_amount
    - tolls_amount
    - ehail_fee
    - improvement_surcharge
    - total_amount
    - payment_type
    - trip_type
    - congestion_surcharge
```

### Handling Both Yellow and Green Taxi Data

If your pipeline handles both taxi types, you have several options:

#### Option 1: Conditional Logic in Kestra

```yaml
- id: create_table
  type: io.kestra.plugin.jdbc.postgresql.Queries
  sql: |
    {% if inputs.taxi == 'green' %}
    CREATE TABLE IF NOT EXISTS {{render(vars.table)}} (
        VendorID text,
        lpep_pickup_datetime timestamp,
        -- ... green schema
    );
    {% else %}
    CREATE TABLE IF NOT EXISTS {{render(vars.table)}} (
        VendorID text,
        tpep_pickup_datetime timestamp,
        -- ... yellow schema
    );
    {% endif %}
```

#### Option 2: Separate Tables

Keep separate tables for each taxi type with their native schemas:
- `yellow_tripdata` with tpep columns
- `green_tripdata` with lpep columns

#### Option 3: Unified Schema with Transformation

Transform data during loading to a unified schema:

```yaml
- id: transform_and_load
  type: io.kestra.plugin.scripts.python.Script
  script: |
    import pandas as pd
    
    df = pd.read_csv(input_file)
    
    # Rename columns to unified names
    if 'lpep_pickup_datetime' in df.columns:
        df = df.rename(columns={
            'lpep_pickup_datetime': 'pickup_datetime',
            'lpep_dropoff_datetime': 'dropoff_datetime'
        })
    else:
        df = df.rename(columns={
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime'
        })
    
    # Add missing columns with nulls
    for col in ['ehail_fee', 'trip_type']:
        if col not in df.columns:
            df[col] = None
    
    df.to_csv(output_file, index=False)
```

### Understanding "lpep" vs "tpep"

These prefixes indicate the electronic payment system used:

- **TPEP (Taxicab Passenger Enhancement Program)**: Used by yellow medallion taxis since 2008
- **LPEP (Livery Passenger Enhancement Program)**: Used by green boro taxis since 2013

The data comes from these electronic meters/payment systems, hence the different column names even though they capture the same information (pickup/dropoff times).

### Quick Reference: Green Taxi Gotchas

1. **lpep not tpep** - Green taxis use `lpep_pickup_datetime`, `lpep_dropoff_datetime`
2. **Extra columns** - `ehail_fee` and `trip_type` exist in green but not yellow
3. **Different column order** - Don't assume columns are in the same order
4. **Trailing commas** - The GitHub-hosted files have trailing commas (20 columns + empty = 21)
5. **Schema changes over time** - Check the year; schemas have evolved

### Key Takeaways

- **Don't assume schema similarity** - Even related datasets can have different structures
- **Always check the actual data** before designing tables
- **Column names must match exactly** when using named column mapping
- **Column order matters** when using positional loading
- **Document dataset quirks** - Future you will thank present you
- **Official data dictionaries are your friend** - When in doubt, check the source
- **Plan for schema evolution** - Datasets change over time

### Related Concepts

- **Schema Design**: Structuring database tables to match data sources
- **Data Profiling**: Analyzing data to understand its structure and quality
- **Schema Evolution**: How data structures change over time
- **Data Dictionaries**: Documentation describing dataset columns and their meanings
- **Dimensional Modeling**: Organizing data for analytics (facts and dimensions)
- **ELT vs ETL**: Extract-Load-Transform vs Extract-Transform-Load patterns

---

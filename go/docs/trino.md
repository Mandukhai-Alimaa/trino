---
# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
{}
---

{{ cross_reference|safe }}
# Trino Driver {{ version }}

{{ version_header|safe }}

This driver provides access to [Trino][trino], a free and
open-source distributed SQL query engine.

## Installation & Quickstart

The driver can be installed with `dbc`.

To use the driver, provide the Trino connection URI as the `url` option.

## Connection URI Format

The Trino ADBC driver supports `trino://` URIs:

```
trino://username[:password]@host[:port][?param1=value1&param2=value2]
```

Components:
- Scheme: trino:// (required)
- Username: Required (for authentication)
- Password: Optional (for authentication)
- Host: Required (no default)
- Port: Optional (defaults to 8080)
- Query params: Trino connection parameters

See [Trino Concepts](https://trino.io/docs/current/overview/concepts.html#catalog) for more information.

Common Parameters:
- `catalog`: Trino catalog name (e.g., `hive`, `postgresql`)
- `schema`: Schema within catalog (e.g., `default`, `public`)
- `source`: Connection source identifier for troubleshooting
- `session_properties`: Comma-separated session properties
- `custom_client`: Name of registered custom HTTP client
- `queryTimeout`: Query timeout duration
- `explicitPrepare`: Use explicit prepared statements (true/false)
- `clientTags`: Comma-separated client tags

Examples:
- trino://user@localhost:8080?catalog=default&schema=test
- trino://user@localhost:8080?source=hello&catalog=default&schema=foobar
- trino://user@localhost:8443?session_properties=query_max_run_time=10m,query_priority=2


## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

[trino]: https://trino.io/

# metabaser

metabaser is an R package to connect to and query a Metabase API.

## Installation

The simplest way to install the package is directly from gitlab using `devtools`.

```
devtools::install_git('https://username:password@gitlab.hres.ca/phac/metabaser.git')

```
Note that this is not completly secure as a password is typed in the clear.
Some work needs to be done to investigate installation using access tokens or over SSH.

## Usage

```
# setup connection
metabase_setup(base_url = "https://atlas-metabase.hres.ca/api",
               database_id = 17, # phac database
               creds_file = "~/metabase_creds")

# login
metabase_status()
metabase_login()
metabase_status()

# query
d <- metabase_query("select * from table limit 10")

# logout
metabase_status()
metabase_logout()
metabase_status()
```

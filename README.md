# metabaser

metabaser is an R package to connect to and query a Metabase API.

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

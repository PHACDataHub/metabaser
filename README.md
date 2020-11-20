# metabaser

`metabaser` is an R package to connect to and query a Metabase API.

## Installation

The simplest way to install the package is directly from gitlab using `devtools`.

```
devtools::install_git('https://username:password@gitlab.hres.ca/phac/metabaser.git')

```
Note that this is not completly secure as a password is typed in the clear.
Some work needs to be done to investigate installation using access tokens (as below) or over SSH.

If installation via `devtools` fails, try using `remotes` (a subset of `devtools` solely for non-CRAN package installation) after setting up an authentication token (TODO: add auth_token instructions):

```
remotes::install_gitlab(repo = "phac/metabaser", host = "gitlab.hres.ca", auth_token = "MY_GENERATED_TOKEN")

```

## Usage

```
# login and setup connection
metabase_login(base_url = "https://discover-metabase.hres.ca/api",
               database_id = 2, # phac database
               creds_file = "~/metabase_creds", # a file with username and password 
               username = "METABASE_USERNAME",
               password = "METABASE_PASSWORD")


# query
df <- metabase_query("select * from table limit 10") 

typed_df <-metabase_query("select * from table", col_types = cols(.default = col_character())) #sets the default type to character to avoid parsing failures

small_df <- metabase_query2("select * from table limit 10") #limits to 2K records, used for quick queries

# logout
metabase_logout()
```

use movine_core::migration::Migration;
use proc_macro::TokenStream;
use quote::quote;

#[proc_macro]
pub fn embed_migrations(input: TokenStream) -> TokenStream {
    let ident = syn::parse_macro_input!(input as syn::Ident);

    let file_handler = movine_core::file_handler::FileHandler::new("./migrations"); // TODO: load from config
    let local_migrations = file_handler.load_local_migrations().expect(&format!("Unable to find migration dir"));

    let migrations: Vec<_> = local_migrations
        .into_iter()
        .map(|migration| {
            let Migration { name, up_sql, down_sql, hash } = migration;
            let up_sql = up_sql.expect("Migration missing up_sql");
            let down_sql = down_sql.expect("Migration missing down_sql");
            let hash = hash.expect("Migration missing hash");
            quote! {
                ::movine::EmbeddedMigration {
                    name: #name,
                    up_sql: #up_sql,
                    down_sql: #down_sql,
                    hash: #hash,
                }
            }
        })
        .collect();

    let count = migrations.len();

    quote! { const #ident: [::movine::EmbeddedMigration; #count] = [ #(#migrations),* ]; }.into()
}

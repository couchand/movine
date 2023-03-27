use movine_core::migration::Migration;
use proc_macro::TokenStream;
use quote::quote;

#[proc_macro]
pub fn embed_migrations(input: TokenStream) -> TokenStream {
    let migration_dir = syn::parse_macro_input!(input as syn::LitStr);

    let migration_dir = migration_dir.value();
    let file_handler = movine_core::file_handler::FileHandler::new(&migration_dir);
    let local_migrations = file_handler.load_local_migrations().expect(&format!("Unable to find migration dir: {migration_dir}"));

    let migrations: Vec<_> = local_migrations
        .into_iter()
        .map(|migration| {
            let Migration { name, up_sql, down_sql, hash } = migration;
            let name = quote!(::std::string::String::from(#name));
            let up_sql = match up_sql {
                None => quote!(::std::option::Option::None),
                Some(s) => quote!(::std::option::Option::Some(::std::string::String::from(#s))),
            };
            let down_sql = match down_sql {
                None => quote!(::std::option::Option::None),
                Some(s) => quote!(::std::option::Option::Some(::std::string::String::from(#s))),
            };
            let hash = match hash {
                None => quote!(::std::option::Option::None),
                Some(s) => quote!(::std::option::Option::Some(::std::string::String::from(#s))),
            };
            quote! {
                ::movine::Migration {
                    name: #name,
                    up_sql: #up_sql,
                    down_sql: #down_sql,
                    hash: #hash,
                }
            }
        })
        .collect();

    quote! { vec![ #(#migrations),* ] }.into()
}

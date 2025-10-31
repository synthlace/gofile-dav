use self_update::cargo_crate_version;

pub fn self_upgrade() -> Result<(), Box<dyn ::std::error::Error>> {
    let mut status_builder = self_update::backends::github::Update::configure();

    status_builder.repo_owner("synthlace");

    status_builder
        .repo_name("gofile-dav")
        .bin_name("gofile-dav")
        .show_download_progress(true)
        .no_confirm(true)
        .current_version(cargo_crate_version!())
        .build()?
        .update()?;

    Ok(())
}

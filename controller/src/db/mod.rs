//! Database access and controller-side database notifications.

mod events;
mod group_routes;
mod groups;
mod init;
mod notifications;
mod schema;

pub use events::DbEvent;
pub use groups::{
    add_group_member, create_group, load_group_directory, load_group_members, remove_group_member,
};
pub use init::init_db;
pub use notifications::{
    setup_flow_notification, setup_group_notification, setup_route_notification,
};

pub(crate) use group_routes::{RecomputedGroupRoutes, recompute_group_routes};

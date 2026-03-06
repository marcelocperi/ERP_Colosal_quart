# Changelog

All notable changes to this project will be documented in this file.

## [2.0.8] - 2026-02-16

### Fixed
- `compras.pagar` route `BuildError`: Implemented missing `/compras/pagar` route and `api_facturas_pendientes_proveedor` endpoint to fix broken link in `ordenes_pago_lista.html`.

## [2.0.7] - 2026-02-15

### Added
- **Separation of Duties (SoD)** implementation.
- `setup_sod_users.py`: Script to create 15 SoD roles and 14 example users with granular permissions.
- `verify_all_tables_eid.py`: Utility to verify `enterprise_id` existence across system tables.
- `.agent/SOD_MATRIX.md`: Comprehensive documentation of SoD roles and conflicts.
- `.agent/SOD_IMPLEMENTATION_SUMMARY.md`: Executive summary of SoD status.

### Fixed
- `utils/menu_loader.py`: Improved error handling for missing/malformed JSON menu structure.
- `setup_sod_users.py`: Rewrote SQL queries to be single-line to avoid parsing issues on Windows environment; corrected permission assignment logic to correctly include `enterprise_id` in `sys_role_permissions`.
- Database schemas now strictly enforce `enterprise_id` in all system tables (`sys_users`, `sys_roles`, `sys_permissions`, `sys_role_permissions`).

## [2.0.6] - 2026-02-15
- Dynamic menu implementation based on distinct business operations.
- Permission-based menu filtering.

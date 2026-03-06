# Mapa Lógico de Conocimiento - Colosal ERP

Este documento sirve como arquitectura de referencia para el LLM Local.

## 1. Arquitectura de Endpoints (Rutas)
| Módulo | Ruta | Función | Métodos |
| :--- | :--- | :--- | :--- |
| core | `/login` | `login` | GET, POST |
| core | `/auth/enforce-change-password` | `enforce_password_change` | GET, POST |
| core | `/profile/change-password` | `change_password` | POST |
| core | `/auth/reset-password` | `reset_password_public` | GET, POST |
| core | `/auth/request-temp-password` | `request_temp_password` | POST |
| core | `/auth/reset/<int:ent_id>/<int:user_id>/<token>` | `reset_with_token` | GET |
| core | `/auth/reset-confirm` | `reset_confirm` | POST |
| core | `/admin/roles/create` | `create_role` | POST |
| core | `/admin/roles/init-sod` | `admin_roles_init_sod` | POST |
| core | `/admin/roles/update_permissions/<int:role_id>` | `update_role_permissions` | POST |
| core | `/admin/roles/revoke` | `revoke_role_permission` | POST |
| core | `/admin/roles/delete/<int:role_id>` | `delete_role` | POST |
| core | `/admin/users/reset-attempts/<int:user_id>` | `reset_user_attempts` | POST |
| core | `/admin/users/reset-password/<int:user_id>` | `admin_reset_password` | POST |
| core | `/admin/users/create` | `create_system_user` | POST |
| core | `/admin/users/update/<int:user_id>` | `update_user` | POST |
| core | `/admin/puestos/create` | `create_puesto` | POST |
| core | `/admin/puestos/update/<int:id>` | `update_puesto` | POST |
| core | `/admin/puestos/delete/<int:id>` | `delete_puesto` | POST |
| core | `/admin/audit/ai-auditor` | `ai_auditor` | GET, POST |
| core | `/admin/empresa/fiscal` | `admin_empresa_fiscal` | GET |
| core | `/admin/empresa/fiscal` | `admin_empresa_fiscal_save` | POST |
| core | `/admin/numeracion/save` | `admin_numeracion_save` | POST |
| core | `/admin/numeracion/delete/<int:id>` | `admin_numeracion_delete` | POST |
| core | `/admin/numeracion/clone` | `admin_numeracion_clone` | POST |
| core | `/admin/tipos-comprobante/save` | `admin_tipos_comprobante_save` | POST |
| core | `/admin/services/control/<int:service_id>` | `control_service` | POST |
| core | `/admin/services/georef/sync` | `admin_georef_sync` | POST |
| core | `/admin/services/rotation` | `admin_rotation_control` | POST |
| core | `/admin/services/config/<int:service_id>` | `admin_service_config` | GET, POST |
| core | `/admin/services/create` | `admin_service_create` | GET, POST |
| core | `/admin/threads/cancel/<task_id>` | `cancel_thread` | POST |
| core | `/admin/error-log/update/<int:error_id>` | `error_log_update_incident` | POST |
| core | `/admin/error-log/quick-action/<int:error_id>` | `error_log_quick_action` | POST |
| compras | `/compras/api/tipos-cambio/manual` | `api_tipos_cambio_manual` | POST |
| compras | `/compras/proveedores/nuevo-internacional` | `nuevo_proveedor_internacional` | GET, POST |
| compras | `/compras/importaciones/documentos/agregar` | `importacion_agregar_documento` | POST |
| compras | `/compras/api/importaciones/orden/<int:orden_id>/marcar` | `api_marcar_orden_importacion` | POST |
| compras | `/compras/cotizacion/<int:id>` | `cotizacion_detalle` | GET, POST |
| compras | `/compras/proveedores/nuevo` | `nuevo_proveedor` | GET, POST |
| compras | `/compras/proveedores/editar/<int:id>` | `editar_proveedor` | POST |
| compras | `/compras/proveedores/toggle-convenio/<int:id>` | `toggle_convenio` | POST |
| compras | `/compras/proveedores/agregar-cm05/<int:id>` | `agregar_cm05` | POST |
| compras | `/compras/proveedores/upload-cm05/<int:id>` | `upload_cm05` | POST |
| compras | `/compras/proveedores/agregar-direccion/<int:id>` | `agregar_direccion` | POST |
| compras | `/compras/proveedores/agregar-contacto/<int:id>` | `agregar_contacto` | POST |
| compras | `/compras/proveedores/agregar-fiscal/<int:id>` | `agregar_fiscal` | POST |
| compras | `/compras/cotizacion/<int:id>/generar_po` | `generar_po` | POST |
| compras | `/compras/orden_nueva` | `orden_nueva` | GET, POST |
| compras | `/compras/orden/<int:id>/agregar_item` | `agregar_item_po` | POST |

## 2. Servicios de Negocio (Core Logic)
- **afip_service.py**: Clases `AfipService`. Métodos clave: 
- **audit_certification_service.py**: Clases `AuditCertificationService`. Métodos clave: 
- **barcode_service.py**: Clases `BarcodeService`. Métodos clave: 
- **bcra_service.py**: Clases `BCRAService, CurrencyRateService`. Métodos clave: 
- **billing_service.py**: Clases `BillingService`. Métodos clave: 
- **book_service_factory.py**: Clases `BookServiceFactory, NativeService, OpenLibraryService`. Métodos clave: get_info, get_info
- **cm05_routes.py**: Clases ``. Métodos clave: 
- **cm05_service.py**: Clases `CM05Service`. Métodos clave: 
- **email_service.py**: Clases ``. Métodos clave: 
- **enterprise_init.py**: Clases ``. Métodos clave: 
- **erp_master_service.py**: Clases `ErpMasterService`. Métodos clave: 
- **finance_service.py**: Clases ``. Métodos clave: 
- **georef_service.py**: Clases `GeorefService`. Métodos clave: 
- **importacion_service.py**: Clases `ImportacionService`. Métodos clave: 
- **library_api_service.py**: Clases ``. Métodos clave: 
- **local_intelligence_service.py**: Clases `LocalIntelligenceService`. Métodos clave: 
- **logistics_service.py**: Clases `LogisticsService`. Métodos clave: 
- **numeration_service.py**: Clases `NumerationService`. Métodos clave: 
- **purchase_order_mailer.py**: Clases `PurchaseOrderMailer`. Métodos clave: __init__, generate_security_hash, create_order_from_quotation, _logic_create_order, generate_excel_po, generate_html_body, send_po_email
- **quotation_mailer.py**: Clases `QuotationMailer`. Métodos clave: __init__, generate_security_hash, generate_excel_attachment, generate_html_body, send_email_real, process_pending_quotations
- **risk_mitigation_service.py**: Clases ``. Métodos clave: 
- **rotation_service.py**: Clases `RotationManager`. Métodos clave: __init__, _initialize_session, set_proxies, rotate, get
- **scraping_service.py**: Clases `CuspideScraper, ReldScraper, AmazonScraper, MercadoLibreScraper`. Métodos clave: get_info, get_info, get_info, get_info
- **sod_service.py**: Clases ``. Métodos clave: 
- **system_service.py**: Clases ``. Métodos clave: 
- **tax_engine.py**: Clases `TaxEngine`. Métodos clave: __init__, get_reglas_para_frontend, calcular, _resolver_impuestos, _logic_resolver_impuestos, _resolver_iibb, _logic_resolver_iibb, _get_alicuotas_vigentes, _logic_get_alicuotas_vigentes, get_config_completa
- **tercero_service.py**: Clases `TerceroService`. Métodos clave: 
- **validation_service.py**: Clases ``. Métodos clave: 
- **vessel_tracking_service.py**: Clases `VesselTrackingService`. Métodos clave: 
- **__init__.py**: Clases ``. Métodos clave: 

## 3. Estructura de Navegación y Permisos

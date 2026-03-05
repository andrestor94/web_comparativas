import re

file_path = r"C:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\web_comparativas_v2- ok\web_comparativas_v2\web_comparativas\templates\pliegos\process_view.html"

with open(file_path, "r", encoding="utf-8") as f:
    html = f.read()

# I want to find the section to replace: 
# from "<!-- TABS NAV -->" up to "    </div>\n</div>\n<script>"

pattern = r"(.*?<!-- TABS NAV -->\s*)(.*?)(</div>\n<script>.*)"
match = re.search(pattern, html, flags=re.DOTALL)

if not match:
    print("Could not find the section to replace")
    exit(1)

pre_tabs = match.group(1)
tabs_and_content = match.group(2)
post_tabs = match.group(3)

# Build the new layout
new_layout = """
    <!-- SPLIT LAYOUT -->
    <div class="row" id="mainSplitter">
        <!-- Main Content Column -->
        <div class="col-lg-7 mb-4">
            
            <div class="accordion accordion-flush shadow-sm" style="border-radius: var(--radius-xl); overflow: hidden; border: 1px solid var(--border-soft); background: var(--bg-surface);" id="pliegoAccordion">
                
                <!-- 1. Información Básica -->
                <div class="accordion-item border-bottom">
                    <h2 class="accordion-header" id="headingBasic">
                        <button class="accordion-button" type="button" data-bs-toggle="collapse" data-bs-target="#collapseBasic" aria-expanded="true">
                            <i class="bi bi-info-circle text-primary me-2"></i> Información Básica
                        </button>
                    </h2>
                    <div id="collapseBasic" class="accordion-collapse collapse show" data-bs-parent="#pliegoAccordion">
                        <div class="accordion-body bg-light">
                            <div class="row">
                                <div class="col-md-6 border-end">
                                    <h6 class="text-uppercase text-muted small fw-bold mb-3">Cabecera del Proceso</h6>
                                    <div class="mb-3">
                                        <div class="info-label">Nombre del proceso</div>
                                        <div class="info-val">{{ render_field(data.summary.process_name) if data.summary.process_name else 'No informado' }}</div>
                                    </div>
                                    <div class="mb-3">
                                        <div class="info-label">Objeto</div>
                                        <div class="info-val">{{ render_field(data.basic_info.object) if data.basic_info.object else 'No informado' }}</div>
                                    </div>
                                    <div class="mb-3">
                                        <div class="info-label">Procedimiento</div>
                                        <div class="info-val">{{ render_field(data.basic_info.selection_procedure) if data.basic_info.selection_procedure else 'No informado' }}</div>
                                    </div>
                                    <div class="mb-3">
                                        <div class="info-label">Valor del Pliego</div>
                                        <div class="info-val">
                                            {% if data.basic_info.pliego_value and data.basic_info.pliego_value.value %}
                                                {{ data.basic_info.pliego_value.value.currency }} {{ data.basic_info.pliego_value.value.amount | peso }}
                                            {% else %}
                                                Sin costo / No informado
                                            {% endif %}
                                        </div>
                                    </div>
                                </div>
                                <div class="col-md-6 px-4">
                                    <h6 class="text-uppercase text-muted small fw-bold mb-3">Contacto y Apertura</h6>
                                    <div class="mb-3">
                                        <div class="info-label">Email de Contacto</div>
                                        <div class="info-val">{{ render_field(data.basic_info.contact_email) if data.basic_info.contact_email else 'No informado' }}</div>
                                    </div>
                                    <div class="mb-3">
                                        <div class="info-label">Lugar de apertura</div>
                                        <div class="info-val">{{ render_field(data.basic_info.physical_address) if data.basic_info.physical_address else 'No informado' }}</div>
                                    </div>
                                    <div class="mb-3">
                                        <div class="info-label">Fecha de apertura</div>
                                        <div class="info-val fw-bold text-danger">{{ render_field(data.basic_info.submission_deadline) if data.basic_info.submission_deadline else 'No informado' }}</div>
                                    </div>
                                    <div class="mb-3">
                                        <div class="info-label">Mantenimiento de Oferta</div>
                                        <div class="info-val">{{ render_field(data.basic_info.offer_maintenance_term_days_business) if data.basic_info.offer_maintenance_term_days_business else 'No informado' }} días hábiles</div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- 2. Renglones y Productos -->
                <div class="accordion-item border-bottom">
                    <h2 class="accordion-header" id="headingItems">
                        <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#collapseItems">
                            <i class="bi bi-list-check text-primary me-2"></i> Productos y Renglones ({{ items|length }})
                        </button>
                    </h2>
                    <div id="collapseItems" class="accordion-collapse collapse" data-bs-parent="#pliegoAccordion">
                        <div class="accordion-body bg-light p-0 p-lg-3">
                            <div class="d-flex justify-content-between align-items-center mb-3">
                                <h6 class="text-uppercase text-muted small fw-bold mb-0">Detalle Exclusivo</h6>
                                <input type="text" id="itemsSearch" class="form-control form-control-sm w-50" placeholder="Buscar producto..." onkeyup="filterItems()">
                            </div>
                            <div class="table-responsive" style="max-height: 400px; overflow-y: auto;">
                                <table class="table table-sm table-hover table-custom" id="itemsTable" style="font-size: 0.85rem;">
                                    <thead class="bg-white sticky-top">
                                        <tr>
                                            <th>Renglón</th><th>Código</th><th>Cat. Prog</th><th>Destino</th><th>Cant.</th><th>UD</th><th>Desc</th>
                                        </tr>
                                    </thead>
                                    <tbody class="bg-white">
                                        {% for item in items %}
                                        <tr>
                                            <td class="fw-bold text-center">{{ item.line_number }}</td>
                                            <td>{{ item.product_code|default('-') }}</td>
                                            <td>{{ item.programmatic_category|default('-') }}</td>
                                            <td>{{ item.delivery_location|default('-') }}</td>
                                            <td class="fw-bold">{{ item.quantity }}</td>
                                            <td>{{ item.unit_of_measure }}</td>
                                            <td>{{ item.name|default('-') }}</td>
                                        </tr>
                                        {% endfor %}
                                    </tbody>
                                </table>
                                <div id="noResults" class="text-center p-3 text-muted d-none">No se encontraron productos coincidentes.</div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- 3. Entrega y Logística -->
                <div class="accordion-item border-bottom">
                    <h2 class="accordion-header" id="headingDelivery">
                        <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#collapseDelivery">
                            <i class="bi bi-truck text-primary me-2"></i> Logística y Entrega
                        </button>
                    </h2>
                    <div id="collapseDelivery" class="accordion-collapse collapse" data-bs-parent="#pliegoAccordion">
                        <div class="accordion-body bg-light">
                            <div class="row">
                                <div class="col-md-6 border-end">
                                    <h6 class="text-uppercase text-muted small fw-bold mb-3">Plazos y Condiciones Técnicas</h6>
                                    <div class="mb-3">
                                        <div class="info-label">Términos Generales</div>
                                        <div class="info-val fs-6 text-muted">{{ render_field(data.delivery.terms) if data.delivery.terms else '-' }}</div>
                                    </div>
                                    <div class="mb-3">
                                        <div class="info-label">Plazo Estimado</div>
                                        <div class="info-val">{{ render_field(data.delivery.plazo) if data.delivery.plazo else '-' }}</div>
                                    </div>
                                    <div class="mb-3">
                                        <div class="info-label">Vencimiento Mínimo Exigido</div>
                                        <div class="info-val">{{ render_field(data.delivery.vencimiento_minimo) if data.delivery.vencimiento_minimo else '-' }}</div>
                                    </div>
                                </div>
                                <div class="col-md-6 ps-4">
                                    <h6 class="text-uppercase text-muted small fw-bold mb-3">Sedes / Destinos</h6>
                                    {% if data.delivery.locations %}
                                        <ul class="list-group list-group-flush border rounded-3 overflow-hidden">
                                        {% for loc in data.delivery.locations %}
                                            <li class="list-group-item bg-white">
                                                <div class="fw-bold text-dark" style="font-size: 0.9rem;">{{ loc.name|default('-') }}</div>
                                                <div class="text-muted small"><i class="bi bi-geo-alt"></i> {{ loc.address|default('Dirección no provista') }}</div>
                                            </li>
                                        {% endfor %}
                                        </ul>
                                    {% else %}
                                        <div class="text-center p-3 text-muted bg-white border rounded">Sin sedes específicas detectadas.</div>
                                    {% endif %}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- 4. Pago, Garantías y Económico -->
                <div class="accordion-item border-bottom">
                    <h2 class="accordion-header" id="headingEco">
                        <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#collapseEco">
                            <i class="bi bi-cash-coin text-primary me-2"></i> Económico-Financiero
                        </button>
                    </h2>
                    <div id="collapseEco" class="accordion-collapse collapse" data-bs-parent="#pliegoAccordion">
                        <div class="accordion-body bg-light">
                            <div class="row">
                                <div class="col-md-6 border-end">
                                    <h6 class="text-uppercase text-muted small fw-bold mb-3">Pagos y Facturación</h6>
                                    <div class="mb-3">
                                        <div class="info-label">Condiciones Generales</div>
                                        <div class="info-val">{{ render_field(data.basic_info.payment_terms) if data.basic_info.payment_terms else '-' }}</div>
                                    </div>
                                    <div class="mb-3">
                                        <div class="info-label">Plazo de anticipo</div>
                                        <div class="info-val">{{ render_field(data.payment.anticipo) if data.payment and data.payment.anticipo else '-' }}</div>
                                    </div>
                                    <div class="mb-3">
                                        <div class="info-label">Formato Factura</div>
                                        <div class="info-val">{{ render_field(data.payment.tipo_factura) if data.payment and data.payment.tipo_factura else '-' }}</div>
                                    </div>
                                </div>
                                <div class="col-md-6 px-4">
                                    <h6 class="text-uppercase text-muted small fw-bold mb-3">Garantías Requeridas</h6>
                                    <div class="mb-3">
                                        <div class="info-label">Mantenimiento de Oferta</div>
                                        <div class="info-val fs-5 fw-bold text-dark">{{ render_field(data.guarantees.offer_maintenance_pct) }}</div>
                                    </div>
                                    <div class="mb-3">
                                        <div class="info-label">Cumplimiento de Contrato</div>
                                        <div class="info-val fs-5 fw-bold text-dark">{{ render_field(data.guarantees.contract_compliance_pct) }}</div>
                                    </div>
                                    <div class="mb-3">
                                        <div class="info-label">Impugnación</div>
                                        <div class="info-val">{{ render_field(data.guarantees.impugnation_pct) }}</div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- 5. Penalidades -->
                <div class="accordion-item border-bottom">
                    <h2 class="accordion-header" id="headingPen">
                        <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#collapsePen">
                            <i class="bi bi-exclamation-triangle text-primary me-2"></i> Penalidades
                        </button>
                    </h2>
                    <div id="collapsePen" class="accordion-collapse collapse" data-bs-parent="#pliegoAccordion">
                        <div class="accordion-body bg-light">
                            <div class="row">
                                <div class="col-md-6 border-end mb-3">
                                    <div class="info-label">Mora</div>
                                    <div class="info-val text-muted small mt-1">{{ render_field(data.penalties.mora) if data.penalties and data.penalties.mora else '-' }}</div>
                                </div>
                                <div class="col-md-6 mb-3 ps-4">
                                    <div class="info-label">Multas Específicas</div>
                                    <div class="info-val text-muted small mt-1">{{ render_field(data.penalties.multas) if data.penalties and data.penalties.multas else '-' }}</div>
                                </div>
                                <div class="col-12 border-top pt-3">
                                    <div class="info-label">Términos Generales</div>
                                    <div class="info-val text-muted small mt-1">{{ render_field(data.penalties.penalidades_generales) if data.penalties and data.penalties.penalidades_generales else '-' }}</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- 6. Sobres / Documentación a Presentar -->
                <div class="accordion-item">
                    <h2 class="accordion-header" id="headingSobres">
                        <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#collapseSobres">
                            <i class="bi bi-envelope-paper text-primary me-2"></i> Requisitos / Sobres a Presentar
                        </button>
                    </h2>
                    <div id="collapseSobres" class="accordion-collapse collapse" data-bs-parent="#pliegoAccordion">
                        <div class="accordion-body bg-light">
                            {% if data.sobres and data.sobres|length > 0 %}
                                {% for sobre in data.sobres %}
                                <div class="card mb-3 shadow-sm" style="border-radius: 12px; border: 1px solid var(--border-soft);">
                                    <div class="card-header bg-white fw-bold d-flex justify-content-between align-items-center">
                                        <div><i class="bi bi-archive me-2"></i>{{ sobre.name }}</div>
                                        <span class="badge bg-secondary">{{ sobre.count }} docs</span>
                                    </div>
                                    <div class="card-body p-0">
                                        <ul class="list-group list-group-flush">
                                        {% for doc in sobre.documents %}
                                            <li class="list-group-item" style="font-size: 0.85rem;">
                                                <div class="d-flex w-100 justify-content-between">
                                                <div>
                                                    <input type="checkbox" class="form-check-input me-2 sobre-check">
                                                    <span class="fw-bold me-2">{{ doc.code }}:</span> 
                                                    {{ doc.description }}
                                                </div>
                                                {% if doc.proof_document %}
                                                    <span class="badge bg-light text-primary border"><i class="bi bi-paperclip"></i> Adjs: {{ doc.proof_document }}</span>
                                                {% endif %}
                                                </div>
                                            </li>
                                        {% endfor %}
                                        </ul>
                                    </div>
                                </div>
                                {% endfor %}
                            {% else %}
                                <div class="text-muted text-center p-4">No se detectaron distribuciones de sobres explícitas.</div>
                            {% endif %}
                        </div>
                    </div>
                </div>

            </div> <!-- End Accordion -->

            <div class="mt-4 border rounded bg-dark p-3" id="debugSection">
                <h6 class="text-white">JSON Source (Auditor)</h6>
                <pre class="bg-dark text-light border p-2 rounded mb-0" style="max-height: 200px; overflow: auto; font-size: 0.75rem;">{{ data | tojson(indent=2) }}</pre>
            </div>

        </div>

        <!-- Evidence Split Viewer Column (PDF) -->
        <div class="col-lg-5 mb-4" id="evidencePane">
            <div class="card h-100 shadow-sm" style="border-radius: var(--radius-xl); border: 1px solid var(--border-soft); background: var(--bg-surface); min-height: 600px; display: flex; flex-direction: column; overflow:hidden;">
                <div class="card-header bg-white border-bottom py-3 d-flex justify-content-between align-items-center">
                    <h6 class="mb-0 fw-bold"><i class="bi bi-file-earmark-pdf text-danger me-2"></i> Visor de Documentos</h6>
                    <button class="btn btn-sm btn-outline-secondary" onclick="document.getElementById('splitPdfViewer').src='about:blank'; document.getElementById('splitPdfViewer').classList.add('d-none'); document.getElementById('viewerEmptyState').classList.remove('d-none');"><i class="bi bi-x"></i> Cerrar</button>
                </div>
                <div class="card-body p-0" style="position: relative; flex: 1; display:flex; flex-direction: column;">
                    <!-- Empty State -->
                    <div id="viewerEmptyState" class="d-flex flex-column align-items-center justify-content-center h-100 text-muted p-5 text-center">
                        <i class="bi bi-search fs-1 mb-3 opacity-25"></i>
                        <p class="mb-0 small">Active el <strong>Modo Auditor</strong> arriba y haga clic en "Evidencia" sobre un dato extraído para validar su trazabilidad en el documento original.</p>
                    </div>
                    <!-- Viewer Iframe -->
                    <iframe id="splitPdfViewer" class="d-none" style="flex:1; width:100%; border:none;" src="about:blank"></iframe>
                </div>
            </div>
        </div>

    </div>
"""

new_html = pre_tabs + new_layout + post_tabs

with open(file_path, "w", encoding="utf-8") as f:
    f.write(new_html)

print("Replacement done successfully.")

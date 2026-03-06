import re

path = 'C:/Users/marce/Documents/GitHub/bibliotecaweb/multiMCP/core/templates/admin_risk_dashboard.html'

with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# Find the position of the first <script> tag that starts the charts
start_marker = '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>'
end_marker = '{% endblock %}'

start_idx = content.find(start_marker)
# Find the LAST occurrence of {% endblock %} to capture everything after the first bad script tag
end_idx = content.rfind(end_marker) + len(end_marker)

if start_idx == -1:
    print("ERROR: start marker not found")
    exit(1)

clean_script = r"""<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script>
(function () {
    // --- Failure Modes Doughnut Chart ---
    const fmLabels = [{% for fm in failure_modes %}'{{ fm.failure_mode or "UNKNOWN" }}'{% if not loop.last %}, {% endif %}{% endfor %}];
    const fmData   = [{% for fm in failure_modes %}{{ fm.total or 0 }}{% if not loop.last %}, {% endif %}{% endfor %}];
    const fmColors = ['#ef4444', '#f59e0b', '#3b82f6', '#8b5cf6', '#10b981'];

    if (document.getElementById('failureModeChart') && fmData.length > 0) {
        new Chart(document.getElementById('failureModeChart'), {
            type: 'doughnut',
            data: {
                labels: fmLabels,
                datasets: [{ data: fmData, backgroundColor: fmColors, borderWidth: 2, borderColor: '#1a1a2e' }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        display: true,
                        position: 'bottom',
                        labels: { color: '#94a3b8', font: { size: 10 }, boxWidth: 12, padding: 6 }
                    }
                },
                cutout: '65%'
            }
        });
    }

    // --- Trend Multi-Line Chart ---
    const trendLabels = [{% for label in trend_data.labels %}'{{ label }}'{% if not loop.last %}, {% endif %}{% endfor %}];

    if (document.getElementById('trendChart') && trendLabels.length > 0) {
        const datasets = [
            {% for ds in trend_data.datasets %}{
                label: '{{ ds.label }}',
                data: {{ ds.data | tojson }},
                borderColor: '{{ ds.color }}',
                backgroundColor: '{{ ds.color }}20',
                fill: {% if 'Total' in ds.label %}true{% else %}false{% endif %},
                tension: 0.4,
                pointRadius: 3,
                borderWidth: {% if 'Total' in ds.label %}3{% else %}2{% endif %}
            }{% if not loop.last %}, {% endif %}
            {% endfor %}
        ];

        new Chart(document.getElementById('trendChart'), {
            type: 'line',
            data: { labels: trendLabels, datasets: datasets },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        display: true,
                        position: 'bottom',
                        labels: {
                            color: '#94a3b8',
                            font: { size: 10 },
                            boxWidth: 12,
                            boxHeight: 4,
                            padding: 8,
                            usePointStyle: true,
                            pointStyle: 'line'
                        }
                    }
                },
                scales: {
                    x: { ticks: { color: '#64748b', font: { size: 9 } }, grid: { color: 'rgba(255,255,255,0.05)' } },
                    y: { ticks: { color: '#64748b', font: { size: 9 } }, grid: { color: 'rgba(255,255,255,0.05)' }, beginAtZero: true }
                }
            }
        });
    }
})();
</script>
{% endblock %}"""

new_content = content[:start_idx] + clean_script

with open(path, 'w', encoding='utf-8') as f:
    f.write(new_content)

print("Done. Script block replaced cleanly.")
print(f"Total lines: {new_content.count(chr(10))}")

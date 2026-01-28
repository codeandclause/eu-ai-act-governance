# EU AI Act Governance Implementation

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

Production-ready Python implementation of EU AI Act compliance controls for ML pipelines.

**📖 Companion repository for:** [From Compliance Framework to Code: Implementing AI Governance in Your ML Pipeline](https://codeandclause.ai/ai-governance-ml-pipeline-implementation/)

---

## Features

- ✅ **Data Lineage Tracking** - Complete provenance from source to model with cryptographic verification (Article 10)
- ✅ **Compliance Deployment Gates** - Automated validation before production deployment (Articles 9-15)
- ✅ **Risk-Based Data Quality** - Validation strictness scales with AI system risk level
- ✅ **Bias Monitoring** - Production monitoring with configurable thresholds
- ✅ **Storage Backends** - PostgreSQL and MongoDB implementations included
- ✅ **Automated Documentation** - Model card generation from training metadata

---

## Quick Start

### Installation

```bash
git clone https://github.com/yourusername/eu-ai-act-governance.git
cd eu-ai-act-governance
pip install -r requirements.txt
```

### Basic Usage

```python
from governance import LineageTracker, ComplianceGate, RiskLevel
from governance.storage_backends import PostgresGovernanceStorage

# Initialize storage
storage = PostgresGovernanceStorage(
    host="localhost",
    database="ai_governance",
    user="postgres",
    password="your_password"
)

# Track data lineage
tracker = LineageTracker(storage)

# Extract data
dataset_id = tracker.track_extraction(
    source="customer_db",
    query="SELECT * FROM users WHERE active=true",
    data=raw_dataframe
)

# Track transformations
cleaned_data = clean_data(raw_dataframe)
tracker.track_transformation(
    step_name="remove_nulls",
    input_data=raw_dataframe,
    output_data=cleaned_data,
    transformation_code="df.dropna()"
)

# Link to trained model
tracker.link_to_model(model_id="model_v1.2")

# Validate compliance before deployment
gate = ComplianceGate(storage, model_registry)
can_deploy, report = gate.validate_deployment(
    model_id="model_v1.2",
    risk_level=RiskLevel.HIGH
)

if can_deploy:
    deploy_model("model_v1.2", "production")
else:
    print(f"Deployment blocked: {report['failures']}")
```

---

## Architecture

This implementation follows a 4-layer governance architecture:

1. **Data Governance Layer** - Data validation, lineage tracking, bias detection
2. **Model Governance Layer** - Model registry, risk classification, documentation
3. **Monitoring & Observability** - Performance monitoring, bias monitoring, alerts
4. **Audit Trail Layer** - Immutable event logging, provenance tracking

See the [full article](https://codeandclause.ai/ai-governance-ml-pipeline-implementation/) for architecture details.

---

## EU AI Act Compliance Mapping

| EU AI Act Article | Requirement | Implementation |
|-------------------|-------------|----------------|
| **Article 9** | Risk management system | `ComplianceGate` with risk-based validation |
| **Article 10** | Data governance & provenance | `LineageTracker` with cryptographic verification |
| **Article 11** | Technical documentation | Automated model card generation |
| **Article 14** | Human oversight | Oversight configuration validation |
| **Article 15** | Accuracy, robustness, security | Performance checks, security assessment validation |

---

## Components

### Data Lineage Tracker

Track complete data provenance from source systems through transformations to model training.

**Key features:**
- Cryptographic hashing for data integrity
- Transformation pipeline recording
- Immutable model-data linking
- Audit-ready evidence

**Example:** See `examples/basic_usage.py`

### Compliance Gate

Automated pre-deployment validation ensuring models meet EU AI Act requirements.

**Validates:**
- Risk assessment completed and recent
- Data lineage complete and verified
- Bias thresholds met
- Documentation complete
- Human oversight configured (high-risk systems)
- Security assessment performed
- Performance meets minimum thresholds

**Example:** See `examples/complete_pipeline.py`

### Storage Backends

Database interfaces for governance metadata storage.

**Included:**
- `PostgresGovernanceStorage` - PostgreSQL with JSONB support
- `MongoGovernanceStorage` - MongoDB for flexible schemas
- `GovernanceStorageBackend` - Abstract base class for custom implementations

---

## Configuration

### Risk-Based Validation

Customize thresholds for your domain:

```python
config = {
    'bias_threshold': 0.10,  # Max demographic parity difference
    'min_accuracy_high_risk': 0.90,  # Stricter for high-risk
    'min_accuracy_limited_risk': 0.75,
    'min_f1_high_risk': 0.85,
    'min_f1_limited_risk': 0.70,
    'max_risk_assessment_age_days': 180,
}

gate = ComplianceGate(db, registry, config=config)
```

### Database Setup

**PostgreSQL:**
```sql
CREATE DATABASE ai_governance;

CREATE TABLE data_lineage (
    dataset_id VARCHAR(255) PRIMARY KEY,
    source_systems TEXT[],
    transformation_pipeline JSONB,
    content_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT NOW()
);

-- See database_schemas/ for complete schemas
```

---

## Examples

### Basic Usage
```bash
python examples/basic_usage.py
```

### Complete Pipeline
```bash
python examples/complete_pipeline.py
```

---

## Testing

```bash
pytest tests/
```

Run with coverage:
```bash
pytest --cov=governance tests/
```

---

## Performance

Governance overhead: **<15ms per prediction**

| Control | Overhead | Optimization |
|---------|----------|--------------|
| Audit logging | <1ms | Async batching |
| Data lineage | 10ms | Deterministic hashing |
| Bias monitoring | ~1ms | 1% sampling |
| Data quality | <1ms | LRU caching |

Tested on: AWS m5.large (2 vCPUs, 8GB RAM), PostgreSQL 14, Python 3.9

---

## Requirements

- Python 3.9+
- PostgreSQL 14+ OR MongoDB 4.4+ (choose one)
- pandas >= 1.5.0
- mlflow >= 2.9.0 (optional, for model registry integration)
- fairlearn >= 0.9.0 (optional, for bias detection)

See `requirements.txt` for complete list.

---

## Documentation

- **Full Implementation Guide:** [Article on Code & Clause](https://codeandclause.ai/ai-governance-ml-pipeline-implementation/)
- **Examples:** See `examples/` directory
- **Tests:** See `tests/` directory

---

## Roadmap

### v1.1 (Planned)
- [ ] MLflow integration examples
- [ ] Great Expectations integration
- [ ] FastAPI deployment endpoint example
- [ ] Automated model card templates
- [ ] Grafana dashboards for monitoring

### v1.2 (Future)
- [ ] Support for Snowflake/BigQuery storage
- [ ] Integration with Evidently AI
- [ ] Kubernetes deployment patterns
- [ ] Multi-cloud deployment examples

---

## Contributing

Contributions welcome! Please:

1. Open an issue to discuss proposed changes
2. Fork the repository
3. Create a feature branch
4. Submit a pull request

See `CONTRIBUTING.md` for detailed guidelines (coming soon).

---

## License

MIT License - See [LICENSE](LICENSE) file for details.

**Free for commercial use** - No restrictions on production deployment.

---

## Citation

If you use this code in research or production:

```bibtex
@software{euaiact_governance2026,
  title={EU AI Act Governance Implementation},
  author={Code \& Clause},
  year={2026},
  url={https://github.com/yourusername/eu-ai-act-governance},
  note={Production-ready Python implementation of EU AI Act compliance controls}
}
```

---

## Support

- **💬 Issues:** [GitHub Issues](https://github.com/yourusername/eu-ai-act-governance/issues)
- **📧 Email:** contact@codeandclause.ai
- **📖 Documentation:** [Full Article](https://codeandclause.ai/ai-governance-ml-pipeline-implementation/)
- **🐦 Twitter:** [@codeandclause](https://twitter.com/codeandclause)

---

## About

Created and maintained by [Code & Clause](https://codeandclause.ai) - Practical AI compliance solutions for engineering teams.

**Need custom implementation or consulting?** Contact us at contact@codeandclause.ai

---

## Related Resources

- [EU AI Act Official Text](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689)
- [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
- [Fairlearn](https://fairlearn.org/)
- [Great Expectations](https://greatexpectations.io/)

---

**⭐ Star this repo if you find it useful!**

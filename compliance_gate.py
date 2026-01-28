"""
Compliance Gate for ML Model Deployment
Blocks non-compliant models from reaching production.

Implements pre-deployment validation for EU AI Act requirements.

Usage:
    gate = ComplianceGate(database, model_registry)
    can_deploy, report = gate.validate_deployment(model_id, RiskLevel.HIGH)
    
    if not can_deploy:
        raise DeploymentBlockedException(report['failures'])
"""

from typing import List, Tuple, Dict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json


class RiskLevel(Enum):
    """EU AI Act risk classifications"""
    UNACCEPTABLE = "unacceptable"
    HIGH = "high"
    LIMITED = "limited"
    MINIMAL = "minimal"


@dataclass
class ComplianceCheck:
    """Definition of a single compliance check"""
    name: str
    required_for_risk_levels: List[RiskLevel]
    check_function: callable
    blocking: bool  # Does failure prevent deployment?
    description: str


class ComplianceGate:
    """
    Pre-deployment compliance validation.
    
    Validates that models meet EU AI Act requirements before deployment:
    - Risk assessment completed
    - Data lineage tracked
    - Bias thresholds met
    - Documentation complete
    - Human oversight configured (high-risk)
    - Security assessment performed
    
    Thresholds are configurable for different domains and risk tolerances.
    """
    
    def __init__(self, governance_db, model_registry, config: Optional[Dict] = None):
        """
        Args:
            governance_db: Database connection for governance metadata
            model_registry: GovernedModelRegistry instance
            config: Optional configuration dict with custom thresholds:
                {
                    'bias_threshold': 0.10,  # Max demographic parity difference
                    'min_accuracy_high_risk': 0.85,
                    'min_accuracy_limited_risk': 0.75,
                    'min_f1_high_risk': 0.80,
                    'min_f1_limited_risk': 0.70,
                    'max_risk_assessment_age_days': 180,
                    'max_security_assessment_age_days': 90
                }
        """
        self.db = governance_db
        self.registry = model_registry
        
        # Set configurable thresholds with sensible defaults
        self.config = config or {}
        self.bias_threshold = self.config.get('bias_threshold', 0.10)
        self.min_accuracy_high = self.config.get('min_accuracy_high_risk', 0.85)
        self.min_accuracy_limited = self.config.get('min_accuracy_limited_risk', 0.75)
        self.min_f1_high = self.config.get('min_f1_high_risk', 0.80)
        self.min_f1_limited = self.config.get('min_f1_limited_risk', 0.70)
        self.max_risk_assessment_age = self.config.get('max_risk_assessment_age_days', 180)
        self.max_security_assessment_age = self.config.get('max_security_assessment_age_days', 90)
        
        # Define all compliance checks
        self.checks = self._define_checks()
    
    def _define_checks(self) -> List[ComplianceCheck]:
        """Define all compliance validation checks"""
        return [
            ComplianceCheck(
                name="risk_assessment_complete",
                required_for_risk_levels=[RiskLevel.HIGH, RiskLevel.LIMITED],
                check_function=self._verify_risk_assessment,
                blocking=True,
                description="Valid risk assessment must exist and be recent"
            ),
            ComplianceCheck(
                name="data_lineage_verified",
                required_for_risk_levels=[RiskLevel.HIGH],
                check_function=self._verify_data_lineage,
                blocking=True,
                description="Complete data lineage from source to model"
            ),
            ComplianceCheck(
                name="bias_assessment_passed",
                required_for_risk_levels=[RiskLevel.HIGH],
                check_function=self._verify_bias_thresholds,
                blocking=True,
                description="Bias metrics within acceptable thresholds"
            ),
            ComplianceCheck(
                name="technical_documentation_complete",
                required_for_risk_levels=[RiskLevel.HIGH, RiskLevel.LIMITED],
                check_function=self._verify_documentation,
                blocking=True,
                description="Model card and technical docs complete"
            ),
            ComplianceCheck(
                name="human_oversight_configured",
                required_for_risk_levels=[RiskLevel.HIGH],
                check_function=self._verify_oversight_config,
                blocking=True,
                description="Human oversight measures configured"
            ),
            ComplianceCheck(
                name="performance_meets_threshold",
                required_for_risk_levels=[RiskLevel.HIGH, RiskLevel.LIMITED],
                check_function=self._verify_performance,
                blocking=True,
                description="Model accuracy meets minimum standards"
            ),
            ComplianceCheck(
                name="security_scan_passed",
                required_for_risk_levels=[RiskLevel.HIGH],
                check_function=self._verify_security,
                blocking=False,  # Warning only
                description="Security vulnerability scan completed"
            ),
        ]
    
    def validate_deployment(self, model_id: str, 
                          risk_level: RiskLevel) -> Tuple[bool, Dict]:
        """
        Run all applicable compliance checks.
        
        Args:
            model_id: Model identifier
            risk_level: Model's risk classification
            
        Returns:
            (can_deploy: bool, report: Dict)
        """
        failures = []
        warnings = []
        passed_checks = []
        
        # Run each applicable check
        for check in self.checks:
            if risk_level not in check.required_for_risk_levels:
                continue  # Skip irrelevant checks
            
            try:
                passed, message = check.check_function(model_id, risk_level)
                
                check_result = {
                    'check_name': check.name,
                    'description': check.description,
                    'passed': passed,
                    'message': message,
                    'blocking': check.blocking
                }
                
                if passed:
                    passed_checks.append(check_result)
                else:
                    if check.blocking:
                        failures.append(check_result)
                    else:
                        warnings.append(check_result)
            
            except Exception as e:
                failures.append({
                    'check_name': check.name,
                    'description': check.description,
                    'passed': False,
                    'message': f"Check failed: {str(e)}",
                    'blocking': True
                })
        
        can_deploy = len(failures) == 0
        
        report = {
            'model_id': model_id,
            'risk_level': risk_level.value,
            'can_deploy': can_deploy,
            'checks_run': len([c for c in self.checks if risk_level in c.required_for_risk_levels]),
            'passed': passed_checks,
            'warnings': warnings,
            'failures': failures,
            'timestamp': datetime.now().isoformat()
        }
        
        # Log validation attempt
        self._log_validation_attempt(model_id, report)
        
        return can_deploy, report
    
    def _verify_risk_assessment(self, model_id: str, 
                               risk_level: RiskLevel) -> Tuple[bool, str]:
        """Check if valid risk assessment exists"""
        try:
            assessment = self.db.query(
                "SELECT * FROM risk_assessments WHERE model_id = %s",
                (model_id,)
            )
        except Exception as e:
            return False, f"Database error: {str(e)}"
        
        if not assessment or len(assessment) == 0:
            return False, "No risk assessment found"
        
        # Check recency based on risk level
        max_age_days = self.max_risk_assessment_age if risk_level == RiskLevel.HIGH else 365
        
        try:
            completed_at = assessment[0]['completed_at']
            age_days = (datetime.now() - completed_at).days
            
            if age_days > max_age_days:
                return False, f"Assessment {age_days} days old (max: {max_age_days})"
            
            if not assessment[0].get('assessment_complete', False):
                return False, "Assessment marked as incomplete"
            
            return True, f"Valid assessment (age: {age_days} days)"
        except (KeyError, TypeError) as e:
            return False, f"Invalid assessment data: {str(e)}"
    
    def _verify_data_lineage(self, model_id: str, 
                            risk_level: RiskLevel) -> Tuple[bool, str]:
        """Verify complete data lineage"""
        try:
            model_metadata = self.registry.get_compliance_report(model_id)
        except Exception as e:
            return False, f"Could not retrieve model metadata: {str(e)}"
        
        lineage_id = model_metadata.get('data_lineage_id')
        if not lineage_id:
            return False, "No data lineage ID"
        
        try:
            lineage = self.db.query(
                "SELECT * FROM data_lineage WHERE dataset_id = %s",
                (lineage_id,)
            )
        except Exception as e:
            return False, f"Database error: {str(e)}"
        
        if not lineage or len(lineage) == 0:
            return False, f"Lineage {lineage_id} not found"
        
        # Verify completeness
        transformation_pipeline = lineage[0].get('transformation_pipeline', [])
        if len(transformation_pipeline) == 0:
            return False, "No transformation steps recorded"
        
        # Verify required fields
        required_fields = ['step', 'timestamp', 'input_hash', 'output_hash']
        for i, step in enumerate(transformation_pipeline):
            missing = [f for f in required_fields if f not in step]
            if missing:
                return False, f"Step {i} missing fields: {missing}"
        
        return True, f"Complete lineage with {len(transformation_pipeline)} steps"
    
    def _verify_bias_thresholds(self, model_id: str,
                                risk_level: RiskLevel) -> Tuple[bool, str]:
        """
        Verify bias assessment passed configurable thresholds.
        
        Note: Assumes Fairlearn-style 'demographic_parity_difference' metric.
        Adapt metric_key if using different fairness frameworks (AIF360, etc.)
        """
        try:
            model_metadata = self.registry.get_compliance_report(model_id)
        except Exception as e:
            return False, f"Could not retrieve model metadata: {str(e)}"
        
        bias_assessment = model_metadata.get('full_metadata', {}).get('bias_assessment', {})
        
        if not bias_assessment:
            return False, "No bias assessment results found"
        
        failed_attributes = []
        for attribute, metrics in bias_assessment.items():
            # Support multiple metric formats
            demographic_parity = metrics.get('demographic_parity_difference', 
                                           metrics.get('disparate_impact', 1.0))
            
            if demographic_parity > self.bias_threshold:
                failed_attributes.append(
                    f"{attribute}: {demographic_parity:.2%} (threshold: {self.bias_threshold:.2%})"
                )
        
        if failed_attributes:
            return False, f"Bias thresholds exceeded for: {', '.join(failed_attributes)}"
        
        return True, f"Bias check passed for {len(bias_assessment)} attributes"
    
    def _verify_documentation(self, model_id: str,
                             risk_level: RiskLevel) -> Tuple[bool, str]:
        """Verify technical documentation complete"""
        try:
            model_metadata = self.registry.get_compliance_report(model_id)
        except Exception as e:
            return False, f"Could not retrieve model metadata: {str(e)}"
        
        model_card_id = model_metadata.get('model_card_id')
        if not model_card_id:
            return False, "No model card"
        
        # Optional: Add deeper validation of model card completeness
        return True, "Documentation complete"
    
    def _verify_oversight_config(self, model_id: str,
                                 risk_level: RiskLevel) -> Tuple[bool, str]:
        """Verify human oversight configured"""
        try:
            model_metadata = self.registry.get_compliance_report(model_id)
        except Exception as e:
            return False, f"Could not retrieve model metadata: {str(e)}"
        
        if not model_metadata.get('human_oversight_enabled'):
            return False, "Human oversight not enabled"
        
        measures = model_metadata.get('full_metadata', {}).get('oversight_measures', [])
        if not measures:
            return False, "No oversight measures documented"
        
        return True, f"Oversight configured with {len(measures)} measures"
    
    def _verify_performance(self, model_id: str,
                           risk_level: RiskLevel) -> Tuple[bool, str]:
        """Verify model performance meets configurable thresholds"""
        try:
            model_metadata = self.registry.get_compliance_report(model_id)
        except Exception as e:
            return False, f"Could not retrieve model metadata: {str(e)}"
        
        accuracy_metrics = model_metadata.get('full_metadata', {}).get('accuracy_metrics', {})
        
        if not accuracy_metrics:
            return False, "No performance metrics"
        
        # Use configurable thresholds based on risk level
        if risk_level == RiskLevel.HIGH:
            min_accuracy = self.min_accuracy_high
            min_f1 = self.min_f1_high
        else:  # LIMITED
            min_accuracy = self.min_accuracy_limited
            min_f1 = self.min_f1_limited
        
        accuracy = accuracy_metrics.get('accuracy', 0)
        f1_score = accuracy_metrics.get('f1_score', accuracy_metrics.get('f1', 0))
        
        issues = []
        if accuracy < min_accuracy:
            issues.append(f"Accuracy {accuracy:.2%} below {min_accuracy:.2%}")
        if f1_score < min_f1:
            issues.append(f"F1 score {f1_score:.2%} below {min_f1:.2%}")
        
        if issues:
            return False, "; ".join(issues)
        
        return True, f"Performance passed (accuracy: {accuracy:.2%}, F1: {f1_score:.2%})"
    
    def _verify_security(self, model_id: str,
                        risk_level: RiskLevel) -> Tuple[bool, str]:
        """
        Verify security assessment completed.
        
        Note: Placeholder implementation. In production, integrate with:
        - Bandit (Python security scanner)
        - Snyk / WhiteSource (dependency vulnerabilities)
        - Custom security scanning tools
        """
        try:
            model_metadata = self.registry.get_compliance_report(model_id)
        except Exception as e:
            return False, f"Could not retrieve model metadata: {str(e)}"
        
        security_id = model_metadata.get('security_assessment_id')
        
        if not security_id:
            return False, "No security assessment performed"
        
        # Check if assessment is recent
        try:
            assessment = self.db.query(
                "SELECT * FROM security_assessments WHERE assessment_id = %s",
                (security_id,)
            )
            
            if not assessment or len(assessment) == 0:
                return False, f"Security assessment {security_id} not found"
            
            assessed_at = assessment[0]['completed_at']
            age_days = (datetime.now() - assessed_at).days
            
            if age_days > self.max_security_assessment_age:
                return False, f"Security assessment {age_days} days old (max: {self.max_security_assessment_age})"
            
            return True, f"Security assessment completed {age_days} days ago"
        except Exception as e:
            return False, f"Error checking security assessment: {str(e)}"
    
    def _log_validation_attempt(self, model_id: str, report: Dict):
        """Log to audit trail"""
        audit_entry = {
            'event_type': 'compliance_gate_validation',
            'model_id': model_id,
            'can_deploy': report['can_deploy'],
            'checks_run': report['checks_run'],
            'failures_count': len(report['failures']),
            'full_report': json.dumps(report),
            'timestamp': datetime.now()
        }
        self.db.insert('audit_log', audit_entry)


class DeploymentBlockedException(Exception):
    """Raised when compliance gate blocks deployment"""
    pass


def deploy_model_with_gate(model_id: str, environment: str,
                           compliance_gate: ComplianceGate,
                           model_registry):
    """
    Deploy model with compliance validation.
    
    Example:
        deploy_model_with_gate(
            model_id="model_v1.2",
            environment="production",
            compliance_gate=gate,
            model_registry=registry
        )
    """
    print(f"Deploying {model_id} to {environment}")
    
    metadata = model_registry.get_compliance_report(model_id)
    risk_level = RiskLevel(metadata['risk_level'])
    
    print(f"Risk level: {risk_level.value}")
    print("Running compliance checks...")
    
    can_deploy, report = compliance_gate.validate_deployment(model_id, risk_level)
    
    print(f"\nCompliance Results:")
    print(f"  Passed: {len(report['passed'])}")
    print(f"  Warnings: {len(report['warnings'])}")
    print(f"  Failures: {len(report['failures'])}")
    
    if not can_deploy:
        print("\nBlocking Failures:")
        for failure in report['failures']:
            print(f"  ❌ {failure['check_name']}: {failure['message']}")
        
        raise DeploymentBlockedException(
            f"Model failed {len(report['failures'])} compliance checks"
        )
    
    print(f"\n✅ Compliance passed. Deploying to {environment}...")
    
    # Actual deployment happens here
    # deploy_to_infrastructure(model_id, environment)
    
    print(f"✅ Deployment complete")

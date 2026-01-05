import json
from typing import Dict, Any
from datetime import datetime
from app.utils.logger import logger


class MCPLogger:
    def __init__(self):
        self.steps = []
        self.current_step = 0

    def log_step(
        self,
        step_type: str,
        title: str,
        data: Dict[str, Any],
        status: str = "in_progress",
    ):
        self.current_step += 1
        step = {
            "step": self.current_step,
            "type": step_type,
            "title": title,
            "timestamp": datetime.now().isoformat(),
            "status": status,
            "data": data,
        }
        self.steps.append(step)

        logger.info("=" * 80)
        logger.info(f"STEP {self.current_step}: {title}")
        logger.info("=" * 80)
        logger.info(f"Type: {step_type} | Status: {status}")
        logger.info(f"Data:\n{json.dumps(data, indent=2, default=str)}")
        logger.info("=" * 80)

    def get_execution_log(self) -> Dict[str, Any]:
        """Get complete execution log"""
        return {"total_steps": self.current_step, "execution_log": self.steps}

    def clear(self):
        """Clear all logs"""
        self.steps = []
        self.current_step = 0


mcp_logger = MCPLogger()

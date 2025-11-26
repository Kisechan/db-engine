// 查询规划模块 (Query Planning)
// 
// 负责将 SQL AST 转换为逻辑计划树。
// 
// 流程：
// 1. AST → Planner → LogicalPlan
// 2. 在此过程中进行 Binding（验证表/列存在性）
// 3. 生成树状的算子计划结构
// 4. 优化器对计划进行优化（谓词下推等）

pub mod logical;
pub mod planner;
pub mod optimizer;
pub mod physical;

pub use logical::{LogicalPlan, JoinType};
pub use planner::{Planner, PlannerError};
pub use optimizer::Optimizer;
pub use physical::{PhysicalPlanner, PhysicalPlannerError};

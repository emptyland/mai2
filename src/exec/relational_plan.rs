use std::ops::DerefMut;
use std::sync::Arc;
use crate::base::{ArenaBox, ArenaStr, ArenaVec};
use crate::exec::db::TableHandle;
use crate::sql::ast::Expression;

pub trait RelationalModel {
    fn finalize(&mut self) {}
    fn child(&self, i: usize) -> Option<ArenaBox<dyn RelationalModel>>;
}

fn finalize(node: &mut dyn RelationalModel) {
    for mut child in ChildrenIter::new(node) {
        finalize(child.deref_mut());
    }
    node.finalize();
}

pub struct ChildrenIter<'a> {
    owns: &'a dyn RelationalModel,
    index: usize
}

impl <'a> ChildrenIter<'a> {
    pub fn new(owns: &'a dyn RelationalModel) -> Self {
        Self {
            owns,
            index: 0
        }
    }
}

impl <'a> Iterator for ChildrenIter<'a> {
    type Item = ArenaBox<dyn RelationalModel>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.owns.child(self.index) {
            Some(node) => {
                self.index += 1;
                Some(node)
            }
            None => None
        }
    }
}

pub struct Projection {

}

pub struct Selection {
    pub filtering_expr: ArenaBox<dyn Expression>,
    pub child: ArenaBox<dyn RelationalModel>,
}

impl RelationalModel for Selection {
    fn child(&self, i: usize) -> Option<ArenaBox<dyn RelationalModel>> {
        if i == 0 {
            Some(self.child.clone())
        } else {
            None
        }
    }
}

pub struct PhysicalTable {
    pub table_ref: Option<Arc<TableHandle>>
}

impl RelationalModel for PhysicalTable {
    fn finalize(&mut self) {
        self.table_ref = None; // Drop Arc<TableHandle>
    }

    fn child(&self, _: usize) -> Option<ArenaBox<dyn RelationalModel>> { None }
}

pub struct Rename {
    pub relational_alias: ArenaStr,
    pub columns_alias: ArenaVec<ArenaStr>,
    pub child: ArenaBox<dyn RelationalModel>,
}
#include "storage/ducklake_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

#include "duckdb/main/settings.hpp"

namespace duckdb {

DuckLakeTransactionManager::DuckLakeTransactionManager(AttachedDatabase &db_p, DuckLakeCatalog &ducklake_catalog)
    : TransactionManager(db_p), ducklake_catalog(ducklake_catalog) {
}

Transaction &DuckLakeTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_shared_ptr<DuckLakeTransaction>(ducklake_catalog, *this, context);
	transaction->Start();
	auto is_internal =
	    context.registered_state->Get<DuckLakeInternalConnectionState>(DuckLakeInternalConnectionState::KEY) != nullptr;
	if (ducklake_catalog.IsInitialized() && !is_internal && !ducklake_catalog.CatalogSnapshot()) {
		auto branch_state = context.registered_state->Get<DuckLakeBranchState>(DuckLakeBranchState::KEY);
		DuckLakeBranchState::Selection selection;
		if (branch_state && branch_state->TryGetSelection(ducklake_catalog.GetOid(), selection)) {
			transaction->SetBranch(selection.branch_id, selection.name);
		}
	}
	// a branch is loaded lazily on first use - its snapshot is fixed at the fork anyway
	if (Settings::Get<ImmediateTransactionModeSetting>(context) && get_snapshot && ducklake_catalog.IsInitialized() &&
	    !is_internal && !transaction->IsOnBranch()) {
		get_snapshot = false;
		// no snapshot loaded yet for this transaction - load it
		transaction->GetSnapshot();
		get_snapshot = true;
	}
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData DuckLakeTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<DuckLakeTransaction>();
	try {
		ducklake_transaction.Commit();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void DuckLakeTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<DuckLakeTransaction>();
	ducklake_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

} // namespace duckdb

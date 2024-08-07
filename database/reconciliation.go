package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/lib/pq"
	"go.opentelemetry.io/otel"
)

func (d Datasource) RecordReconciliation(ctx context.Context, rec *model.Reconciliation) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Saving reconciliation to db")
	defer span.End()

	_, err := d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.reconciliations(
			reconciliation_id, upload_id, status, matched_transactions, 
			unmatched_transactions, started_at, completed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		rec.ReconciliationID, rec.UploadID, rec.Status, rec.MatchedTransactions,
		rec.UnmatchedTransactions, rec.StartedAt, rec.CompletedAt,
	)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record reconciliation", err)
	}

	return nil
}

func (d Datasource) GetReconciliation(ctx context.Context, id string) (*model.Reconciliation, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Fetching reconciliation from db")
	defer span.End()

	rec := &model.Reconciliation{}
	err := d.Conn.QueryRowContext(ctx, `
		SELECT id, reconciliation_id, upload_id, status, matched_transactions, 
			unmatched_transactions, started_at, completed_at
		FROM blnk.reconciliations
		WHERE reconciliation_id = $1
	`, id).Scan(
		&rec.ID, &rec.ReconciliationID, &rec.UploadID, &rec.Status,
		&rec.MatchedTransactions, &rec.UnmatchedTransactions,
		&rec.StartedAt, &rec.CompletedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Reconciliation with ID '%s' not found", id), err)
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve reconciliation", err)
	}

	return rec, nil
}

func (d Datasource) UpdateReconciliationStatus(ctx context.Context, id string, status string, matchedCount, unmatchedCount int) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Updating reconciliation status")
	defer span.End()

	completedAt := sql.NullTime{Time: time.Now(), Valid: status == "completed"}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.reconciliations
		SET status = $2, matched_transactions = $3, unmatched_transactions = $4, completed_at = $5
		WHERE reconciliation_id = $1
	`, id, status, matchedCount, unmatchedCount, completedAt)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update reconciliation status", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Reconciliation with ID '%s' not found", id), nil)
	}

	return nil
}

func (d Datasource) GetReconciliationsByUploadID(ctx context.Context, uploadID string) ([]*model.Reconciliation, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Fetching reconciliations by upload ID")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, reconciliation_id, upload_id, status, matched_transactions, 
			unmatched_transactions, started_at, completed_at
		FROM blnk.reconciliations
		WHERE upload_id = $1
		ORDER BY started_at DESC
	`, uploadID)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve reconciliations", err)
	}
	defer rows.Close()

	var reconciliations []*model.Reconciliation

	for rows.Next() {
		rec := &model.Reconciliation{}
		err = rows.Scan(
			&rec.ID, &rec.ReconciliationID, &rec.UploadID, &rec.Status,
			&rec.MatchedTransactions, &rec.UnmatchedTransactions,
			&rec.StartedAt, &rec.CompletedAt,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan reconciliation data", err)
		}

		reconciliations = append(reconciliations, rec)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over reconciliations", err)
	}

	return reconciliations, nil
}

func (d Datasource) RecordMatches(ctx context.Context, reconciliationID string, matches []model.Match) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Batch saving matches to db")
	defer span.End()

	txn, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to start transaction", err)
	}
	defer func() {
		if err := txn.Rollback(); err != nil && err != sql.ErrTxDone {
			span.RecordError(fmt.Errorf("error rolling back transaction: %w", err))
		}
	}()

	stmt, err := txn.PrepareContext(ctx, pq.CopyIn("blnk.matches", "external_transaction_id", "internal_transaction_id", "reconciliation_id", "amount", "date"))
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to prepare statement", err)
	}
	defer stmt.Close()

	for _, match := range matches {
		_, err := stmt.ExecContext(ctx,
			match.ExternalTransactionID,
			match.InternalTransactionID,
			reconciliationID,
			match.Amount,
			match.Date,
		)
		if err != nil {
			return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to execute statement", err)
		}
	}

	_, err = stmt.ExecContext(ctx)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to flush batch insert", err)
	}

	if err := txn.Commit(); err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	return nil
}

func (d Datasource) RecordMatch(ctx context.Context, match *model.Match) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Saving match to db")
	defer span.End()

	_, err := d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.matches(
			external_transaction_id, internal_transaction_id, reconciliation_id, amount, date
		) VALUES ($1, $2, $3, $4, $5)`,
		match.ExternalTransactionID, match.InternalTransactionID, match.ReconciliationID, match.Amount, match.Date,
	)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record match", err)
	}

	return nil
}

func (d Datasource) GetMatchesByReconciliationID(ctx context.Context, reconciliationID string) ([]*model.Match, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Fetching matches by reconciliation ID")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT m.external_transaction_id, m.internal_transaction_id, m.amount, m.date
		FROM blnk.matches m
		JOIN blnk.external_transactions et ON m.external_transaction_id = et.id
		WHERE et.reconciliation_id = $1
	`, reconciliationID)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve matches", err)
	}
	defer rows.Close()

	var matches []*model.Match

	for rows.Next() {
		match := &model.Match{}
		err = rows.Scan(
			&match.ExternalTransactionID, &match.InternalTransactionID,
			&match.Amount, &match.Date,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan match data", err)
		}

		matches = append(matches, match)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over matches", err)
	}

	return matches, nil
}

func (d Datasource) RecordExternalTransaction(ctx context.Context, tx *model.ExternalTransaction, uploadID string) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Saving external transaction to db")
	defer span.End()

	_, err := d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.external_transactions(
			id, amount, reference, currency, description, date, source, upload_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		tx.ID, tx.Amount, tx.Reference, tx.Currency, tx.Description, tx.Date, tx.Source, uploadID,
	)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record external transaction", err)
	}

	return nil
}

func (d Datasource) GetExternalTransactionsByReconciliationID(ctx context.Context, reconciliationID string) ([]*model.ExternalTransaction, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Fetching external transactions by reconciliation ID")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, amount, reference, currency, description, date, source
		FROM blnk.external_transactions
		WHERE reconciliation_id = $1
	`, reconciliationID)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve external transactions", err)
	}
	defer rows.Close()

	var transactions []*model.ExternalTransaction

	for rows.Next() {
		tx := &model.ExternalTransaction{}
		err = rows.Scan(
			&tx.ID, &tx.Amount, &tx.Reference, &tx.Currency,
			&tx.Description, &tx.Date, &tx.Source,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan external transaction data", err)
		}

		transactions = append(transactions, tx)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over external transactions", err)
	}

	return transactions, nil
}

func (d Datasource) RecordMatchingRule(ctx context.Context, rule *model.MatchingRule) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Saving matching rule to db")
	defer span.End()

	criteriaJSON, err := json.Marshal(rule.Criteria)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal matching rule criteria", err)
	}

	_, err = d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.matching_rules(
			rule_id, created_at, updated_at, name, description, criteria
		) VALUES ($1, $2, $3, $4, $5, $6)`,
		rule.RuleID, rule.CreatedAt, rule.UpdatedAt, rule.Name, rule.Description, criteriaJSON,
	)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record matching rule", err)
	}

	return nil
}

func (d Datasource) GetMatchingRules(ctx context.Context) ([]*model.MatchingRule, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Fetching matching rules")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, rule_id, created_at, updated_at, name, description, criteria
		FROM blnk.matching_rules
	`)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve matching rules", err)
	}
	defer rows.Close()

	var rules []*model.MatchingRule

	for rows.Next() {
		rule := &model.MatchingRule{}
		var criteriaJSON []byte
		err = rows.Scan(
			&rule.ID, &rule.RuleID, &rule.CreatedAt, &rule.UpdatedAt,
			&rule.Name, &rule.Description, &criteriaJSON,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan matching rule data", err)
		}

		err = json.Unmarshal(criteriaJSON, &rule.Criteria)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal matching rule criteria", err)
		}

		rules = append(rules, rule)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over matching rules", err)
	}

	return rules, nil
}

func (d Datasource) UpdateMatchingRule(ctx context.Context, rule *model.MatchingRule) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Updating matching rule")
	defer span.End()

	criteriaJSON, err := json.Marshal(rule.Criteria)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal matching rule criteria", err)
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.matching_rules
		SET name = $2, description = $3, criteria = $4
		WHERE rule_id = $1
	`, rule.RuleID, rule.Name, rule.Description, criteriaJSON)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update matching rule", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Matching rule with ID '%s' not found", rule.RuleID), nil)
	}

	return nil
}

func (d Datasource) DeleteMatchingRule(ctx context.Context, id string) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Deleting matching rule")
	defer span.End()

	result, err := d.Conn.ExecContext(ctx, `
		DELETE FROM blnk.matching_rules
		WHERE rule_id = $1
	`, id)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to delete matching rule", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Matching rule with ID '%s' not found", id), nil)
	}

	return nil
}

func (d Datasource) GetMatchingRule(ctx context.Context, id string) (*model.MatchingRule, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Fetching matching rule")
	defer span.End()

	var rule model.MatchingRule
	var criteriaJSON []byte

	err := d.Conn.QueryRowContext(ctx, `
		SELECT id, rule_id, created_at, updated_at, name, description, criteria
		FROM blnk.matching_rules
		WHERE rule_id = $1
	`, id).Scan(
		&rule.ID, &rule.RuleID, &rule.CreatedAt, &rule.UpdatedAt,
		&rule.Name, &rule.Description, &criteriaJSON,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Matching rule with ID '%s' not found", id), err)
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve matching rule", err)
	}

	err = json.Unmarshal(criteriaJSON, &rule.Criteria)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal matching rule criteria", err)
	}

	return &rule, nil
}

func (d Datasource) GetExternalTransactionsPaginated(ctx context.Context, uploadID string, batchSize int, offset int64) ([]*model.ExternalTransaction, error) {
	ctx, span := otel.Tracer("Queue transaction").Start(ctx, "Fetching external transactions with pagination")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, amount, reference, currency, description, date, source
		FROM blnk.external_transactions
		WHERE upload_id = $1
		ORDER BY date DESC
		LIMIT $2 OFFSET $3
	`, uploadID, batchSize, offset)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve external transactions", err)
	}
	defer rows.Close()

	var transactions []*model.ExternalTransaction

	for rows.Next() {
		tx := &model.ExternalTransaction{}
		err = rows.Scan(
			&tx.ID, &tx.Amount, &tx.Reference, &tx.Currency,
			&tx.Description, &tx.Date, &tx.Source,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan external transaction data", err)
		}

		transactions = append(transactions, tx)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over external transactions", err)
	}

	return transactions, nil
}

func (d Datasource) SaveReconciliationProgress(ctx context.Context, reconciliationID string, progress model.ReconciliationProgress) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Saving reconciliation progress to db")
	defer span.End()

	progressJSON, err := json.Marshal(progress)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal reconciliation progress", err)
	}

	_, err = d.Conn.ExecContext(ctx, `
		INSERT INTO blnk.reconciliation_progress (reconciliation_id, progress)
		VALUES ($1, $2)
		ON CONFLICT (reconciliation_id) DO UPDATE
		SET progress = $2
	`, reconciliationID, progressJSON)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to save reconciliation progress", err)
	}

	return nil
}

func (d Datasource) LoadReconciliationProgress(ctx context.Context, reconciliationID string) (model.ReconciliationProgress, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Loading reconciliation progress from db")
	defer span.End()

	var progressJSON []byte
	err := d.Conn.QueryRowContext(ctx, `
		SELECT progress
		FROM blnk.reconciliation_progress
		WHERE reconciliation_id = $1
	`, reconciliationID).Scan(&progressJSON)

	if err != nil {
		if err == sql.ErrNoRows {
			return model.ReconciliationProgress{}, nil // Return empty progress if not found
		}
		return model.ReconciliationProgress{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to load reconciliation progress", err)
	}

	var progress model.ReconciliationProgress
	err = json.Unmarshal(progressJSON, &progress)
	if err != nil {
		return model.ReconciliationProgress{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal reconciliation progress", err)
	}

	return progress, nil
}
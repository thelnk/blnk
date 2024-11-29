/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package api

import (
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"

	model2 "github.com/jerry-enebeli/blnk/api/model"
	"github.com/jerry-enebeli/blnk/model"

	"github.com/gin-gonic/gin"
)

// RecordTransaction handles the recording of a new transaction.
// It binds the incoming JSON request to a RecordTransaction object, validates it,
// and then records the transaction. If any errors occur during validation or recording,
// it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or validating the transaction.
// - 201 Created: If the transaction is successfully recorded.
func (a Api) RecordTransaction(c *gin.Context) {
	var newTransaction model2.RecordTransaction
	// Bind the incoming JSON request to the newTransaction model
	if err := c.ShouldBindJSON(&newTransaction); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid input format",
			"details": err.Error(),
			"code": "INVALID_INPUT",
		})
		return
	}

	// Validate the transaction data
	err := newTransaction.ValidateRecordTransaction()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Validation failed",
			"details": err.Error(),
			"code": "VALIDATION_ERROR",
		})
		return
	}

	// Record the transaction using the Blnk service
	resp, err := a.blnk.RecordTransaction(c.Request.Context(), newTransaction.ToTransaction())
	if err != nil {
		errorCode := "TRANSACTION_ERROR"
		if strings.Contains(err.Error(), "reference") {
			errorCode = "DUPLICATE_REFERENCE"
		} else if strings.Contains(err.Error(), "insufficient funds") {
			errorCode = "INSUFFICIENT_FUNDS"
		} else if strings.Contains(err.Error(), "invalid status") {
			errorCode = "INVALID_STATUS"
		}
		
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Failed to record transaction",
			"details": err.Error(),
			"code": errorCode,
		})
		return
	}

	// Return a response with the recorded transaction
	c.JSON(http.StatusCreated, resp)
}

// QueueTransaction handles queuing a new transaction for later processing.
// It binds the incoming JSON request to a RecordTransaction object, validates it,
// and then queues the transaction. If any errors occur during validation or queuing,
// it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or validating the transaction.
// - 201 Created: If the transaction is successfully queued.
func (a Api) QueueTransaction(c *gin.Context) {
	var newTransaction model2.RecordTransaction
	// Bind the incoming JSON request to the newTransaction model
	if err := c.ShouldBindJSON(&newTransaction); err != nil {
		logrus.Error(err)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid input format",
			"details": err.Error(),
			"code": "INVALID_INPUT",
		})
		return
	}

	// Validate the transaction data
	err := newTransaction.ValidateRecordTransaction()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Validation failed",
			"details": err.Error(),
			"code": "VALIDATION_ERROR",
		})
		return
	}

	// Queue the transaction using the Blnk service
	resp, err := a.blnk.QueueTransaction(c.Request.Context(), newTransaction.ToTransaction())
	if err != nil {
		logrus.Error(err)
		errorCode := "QUEUE_ERROR"
		if strings.Contains(err.Error(), "reference") {
			errorCode = "DUPLICATE_REFERENCE"
		} else if strings.Contains(err.Error(), "insufficient funds") {
			errorCode = "INSUFFICIENT_FUNDS"
		} else if strings.Contains(err.Error(), "invalid status") {
			errorCode = "INVALID_STATUS"
		}
		
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Failed to queue transaction",
			"details": err.Error(),
			"code": errorCode,
		})
		return
	}

	// Return a response with the queued transaction
	c.JSON(http.StatusCreated, resp)
}

// RefundTransaction processes a refund for a transaction based on the given ID.
// It retrieves the transaction to be refunded and processes it in batches. If any errors
// occur during retrieval or processing, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in retrieving the transaction or no transaction is found to refund.
// - 201 Created: If the refund is successfully processed.
func (a Api) RefundTransaction(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Missing transaction ID",
			"details": "id is required. pass id in the route /:id",
			"code": "MISSING_ID",
		})
		return
	}
	transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, 0, 1, false, a.blnk.GetRefundableTransactionsByParentID, a.blnk.RefundWorker)
	if err != nil {
		errorCode := "REFUND_ERROR"
		if strings.Contains(err.Error(), "not in a state that can be refunded") {
			errorCode = "INVALID_STATUS"
		} else if strings.Contains(err.Error(), "transaction not found") {
			errorCode = "NOT_FOUND"
		}
		
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Failed to refund transaction",
			"details": err.Error(),
			"code": errorCode,
		})
		return
	}
	if len(transaction) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "No transaction found",
			"details": "no transaction to refund",
			"code": "NOT_FOUND",
		})
		return
	}
	resp := transaction[0]
	c.JSON(http.StatusCreated, resp)
}

// GetTransaction retrieves a transaction by its ID.
// It returns the transaction details if found. If the ID is not provided or an error
// occurs while retrieving the transaction, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in retrieving the transaction or the ID is missing.
// - 200 OK: If the transaction is successfully retrieved.
func (a Api) GetTransaction(c *gin.Context) {
	id, passed := c.Params.Get("id")

	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Missing transaction ID",
			"details": "id is required. pass id in the route /:id",
			"code": "MISSING_ID",
		})
		return
	}

	resp, err := a.blnk.GetTransaction(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Failed to retrieve transaction",
			"details": err.Error(),
			"code": "TRANSACTION_NOT_FOUND",
		})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// UpdateInflightStatus updates the status of an inflight transaction based on the provided ID and status.
// It processes the transaction in batches according to the specified status (commit or void).
// If any errors occur during processing or if the status is unsupported, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in updating the status or if the ID or status is missing or unsupported.
// - 200 OK: If the inflight transaction status is successfully updated.
func (a Api) UpdateInflightStatus(c *gin.Context) {
	var resp *model.Transaction
	id, passed := c.Params.Get("txID")
	var req model2.InflightUpdate
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Missing transaction ID",
			"details": "id is required. pass id in the route /:id",
			"code": "MISSING_ID",
		})
		return
	}
	err := c.BindJSON(&req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid input format",
			"details": err.Error(),
			"code": "INVALID_INPUT",
		})
		return
	}

	status := req.Status
	if status == "commit" {
		transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, req.Amount, 1, false, a.blnk.GetInflightTransactionsByParentID, a.blnk.CommitWorker)
		if err != nil {
			errorCode := "COMMIT_ERROR"
			if strings.Contains(err.Error(), "not in inflight status") {
				errorCode = "INVALID_STATUS"
			} else if strings.Contains(err.Error(), "already committed") {
				errorCode = "ALREADY_COMMITTED"
			}
			
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Failed to commit transaction",
				"details": err.Error(),
				"code": errorCode,
			})
			return
		}
		if len(transaction) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "No transaction found",
				"details": "no transaction to commit",
				"code": "NOT_FOUND",
			})
			return
		}
		resp = transaction[0]
	} else if status == "void" {
		transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, req.Amount, 1, false, a.blnk.GetInflightTransactionsByParentID, a.blnk.VoidWorker)
		if err != nil {
			errorCode := "VOID_ERROR"
			if strings.Contains(err.Error(), "not in inflight status") {
				errorCode = "INVALID_STATUS"
			} else if strings.Contains(err.Error(), "already voided") {
				errorCode = "ALREADY_VOIDED"
			}
			
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Failed to void transaction",
				"details": err.Error(),
				"code": errorCode,
			})
			return
		}
		if len(transaction) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "No transaction found",
				"details": "no transaction to void",
				"code": "NOT_FOUND",
			})
			return
		}
		resp = transaction[0]
	} else {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid status",
			"details": "status not supported. use either commit or void",
			"code": "INVALID_STATUS",
		})
		return
	}

	c.JSON(http.StatusOK, resp)
}

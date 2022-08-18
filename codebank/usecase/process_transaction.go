package usecase

import (
	"encoding/json"
	"time"

	"github.com/codeedu/codebank/domain"
	"github.com/codeedu/codebank/dto"
	"github.com/codeedu/codebank/infrastructure/kafka"
)

type UseCaseTransaction struct {
	TransactionRepository domain.TransactionRepository
	KafkaProducer         kafka.KafkaProducer
}

func NewUseCaseTransaction(transactionRepository domain.TransactionRepository) UseCaseTransaction {
	return UseCaseTransaction{TransactionRepository: transactionRepository}
}

func (u UseCaseTransaction) ProcessTransaction(transactionDto dto.Transaction) (domain.Transaction, error) {
	creditCard := u.hydrateCreditCard(transactionDto)
	ccBalanceAdnLimit, err := u.TransactionRepository.GetCreditCard(*creditCard)
	if err != nil {
		return domain.Transaction{}, err
	}
	creditCard.ID = ccBalanceAdnLimit.ID
	creditCard.Limit = ccBalanceAdnLimit.Limit
	creditCard.Balance = ccBalanceAdnLimit.Balance
	t := u.newTransaction(transactionDto, ccBalanceAdnLimit)
	t.ProcessAndValidate(creditCard)
	err = u.TransactionRepository.SaveTransaction(*t, *creditCard)
	if err != nil {
		return domain.Transaction{}, err
	}
	//Tratando e publicando a transação no Kafka
	transactionDto.ID = t.ID
	transactionDto.CreatedAt = t.CreatedAt
	transactionJson, err := json.Marshal(transactionDto)
	if err != nil {
		return domain.Transaction{}, err
	}
	err = u.KafkaProducer.Publish(string(transactionJson), "payments")
	if err != nil {
		return domain.Transaction{}, err
	}
	return *t, nil
}

func (UseCaseTransaction) hydrateCreditCard(transactionDto dto.Transaction) *domain.CreditCard {
	creditCard := domain.NewCreditCard()
	creditCard.Name = transactionDto.Name
	creditCard.Number = transactionDto.Number
	creditCard.ExpirationMonth = transactionDto.ExpirationMonth
	creditCard.ExpirationYear = transactionDto.ExpirationYear
	creditCard.CVV = transactionDto.CVV
	return creditCard
}

func (u UseCaseTransaction) newTransaction(transaction dto.Transaction, cc domain.CreditCard) *domain.Transaction {
	t := domain.NewTransaction()
	t.CreditCardId = cc.ID
	t.Amount = transaction.Amount
	t.Store = transaction.Store
	t.Description = transaction.Description
	t.CreatedAt = time.Now()
	return t
}

# Controlled eToro cost experiment

Purpose: validate Agent Portfolio scaling and eToro CFD transaction-cost accounting using a short-lived supervised real-money trade.

## Known Agent Portfolio scaling

- Agent Portfolio virtual balance: USD 10,000
- Funded copy amount: USD 200
- Copy scale: 0.02 real USD per virtual USD
- Broker API amounts are virtual Agent Portfolio amounts.

## Proposed experiment

- Instrument: CRV (same instrument as the previous observed trade)
- Direction: LONG solely for experimental consistency, not as a directional market view
- Leverage: x1
- Target funded capital: approximately EUR 50 (converted to USD at preparation time)
- Broker virtual capital: funded USD / 0.02
- Holding period: approximately 2 hours, with explicit time-based manual close
- Mandatory SL/TP around the live entry to bound market movement during the experiment
- No overnight hold

## Fee hypothesis

Current eToro documentation states crypto CFD spread fees are 1% per trade. The working hypothesis is therefore approximately 1% of position value on opening plus 1% on closing, with the closing fee adjusted to the closing market value.

The experiment should record, before opening and after closing:

- broker credit/equity
- live bid/ask and execution rates
- position ID / order ID
- investment / initialInvestment
- units
- trade-history netProfit
- trade-history fees
- what-if costs returned before submission
- any account statement cost lines available after close

Derived values to compare:

- gross market PnL = (closeRate - openRate) * units for LONG
- documented opening spread fee
- documented closing spread fee using close value
- observed broker-credit delta
- real-money equivalents using the 0.02 copy scale

Do not use this experiment path to weaken normal autonomous risk gates or change their caps.
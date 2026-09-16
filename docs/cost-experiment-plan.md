# Controlled eToro cost experiment

Purpose: validate Agent Portfolio scaling and eToro CFD transaction-cost accounting using a short-lived supervised real-money trade.

## Known Agent Portfolio scaling

- Agent Portfolio virtual balance: approximately USD 10,000
- Funded copy amount: USD 200
- Current copy scale: approximately 0.02 real USD per virtual USD
- Broker API amounts are virtual Agent Portfolio amounts.

## Proposed experiment

- Instrument: AAPL
- Direction: LONG solely for the controlled experiment, not as a directional market view
- Leverage: x5, therefore CFD
- Target funded capital: EUR 50 converted to USD at preparation time
- Virtual margin: the same fraction of current Agent Portfolio virtual equity as the target real USD is of the USD 200 funded copy amount
- Holding period: approximately 2 hours, with explicit time-based manual close
- Mandatory stop loss no more than 2% below the live ask; initial test uses 1.5%
- No take profit: close is time-based for cleaner cost analysis
- No overnight hold

## Fee hypothesis

Current eToro documentation states stock and ETF CFDs charge 0.15% per transaction. The working hypothesis is therefore approximately 0.15% of leveraged position value on opening plus 0.15% on closing, in addition to the natural market bid/ask spread and any execution slippage. No overnight financing should apply if the position is closed the same session.

The experiment records, before opening and after closing:

- broker credit/equity
- live bid/ask and execution rates
- position ID / order ID
- virtual margin and x5 exposure
- real-money equivalent using the copy scale
- investment / initialInvestment
- units
- trade-history netProfit
- trade-history fees
- what-if costs returned before submission
- any account statement cost lines available after close

Derived values to compare:

- gross market PnL = (closeRate - openRate) * units for LONG
- expected 0.15% opening CFD charge
- expected 0.15% closing CFD charge using close position value
- natural bid/ask spread and slippage
- observed broker-credit delta
- virtual-to-real scaled equivalents

The experiment has isolated caps: at most 30% of funded real capital and at most 150% of virtual Agent Portfolio equity as leveraged exposure. It does not alter normal autonomous risk configuration or caps.
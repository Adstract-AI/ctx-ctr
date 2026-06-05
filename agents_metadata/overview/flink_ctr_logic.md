# Flink Domain Logic - Real-Time CTR Updating

## Purpose

This component is responsible for maintaining continuously updated CTR estimates using the Bayesian CTR framework.

The component consumes impression and click events from Kafka and updates CTR statistics in real time.

---

## Bucket Definition

CTR is maintained per:

text (ad_category,  publisher_domain,  conversation_category) 

This tuple is referred to as a CTR bucket.

Each bucket maintains its own Bayesian posterior.

---

## State Maintained Per Bucket

For every bucket:

text I C  alpha_prior beta_prior  alpha_posterior beta_posterior  ctr  variance  ci_low ci_high  trusted 

---

## Event Types

### Impression Event

json {   "event_type": "impression",   "ad_category": "...",   "publisher_domain": "...",   "conversation_category": "..." } 

Updates:

text I += 1 

---

### Click Event

json {   "event_type": "click",   ... } 

Updates:

text C += 1 

---

## Prior Construction

The prior mean is computed from:

text w0 w_ad w_dom w_ctx 

using:

m0 = sigmoid(
    w0
    + w_ad
    + w_dom
    + w_ctx
)

The corresponding Beta prior parameters are:

text alpha0 = m0 * s beta0  = (1-m0) * s 

where s is the prior strength.

---

## Posterior Update

For each bucket:

text alpha' = alpha0 + C  beta' = beta0 + I - C 

Posterior CTR:

text ctr = alpha' / (alpha' + beta') 

---

## Variance Calculation

Compute posterior variance using the Beta variance formula.

Variance is used to determine stability.

---

## Confidence Interval

Compute:

text CTR ± z * sqrt(variance) 

where z corresponds to the selected confidence level.

---

## Stability Guards

A bucket becomes trusted only if:

text I >= I_min  variance <= variance_max  CI_width <= CI_max 

Otherwise it remains untrusted.

---

## Storage

Updated bucket state is written to Redis.

Recommended key:

text ctr: ad_category: publisher_domain: conversation_category 

---

## Update Frequency

Updates occur continuously as Kafka events arrive.

Target latency:

text 1-5 seconds 

from event arrival to Redis update.
# Spark Domain Logic - Periodic Weight Recalibration

## Purpose

This component implements the weight-learning procedure described in the CTR framework.

Unlike CTR estimation, weight updates are not performed continuously.

Weights are recalibrated periodically using accumulated evidence.

Recommended cadence:

text Every 1 hour 

---

## Parameters Learned

Global baseline:

text w0 

Feature families:

text w_ad[ad_category]  w_dom[publisher_domain]  w_ctx[conversation_category] 

---

## Input Data

Read from Redis/PostgreSQL:

text I_k C_k  current weight  current baseline 

for all buckets within each family.

---

## Family Processing

Each family is processed independently.

Example:

text ad_category 

Buckets:

text health travel finance education ... 

Each bucket receives its own update.

The same process is repeated for:

text publisher_domain  conversation_category 

---

## Prior Construction

For bucket k:

text m0_k = sigmoid(     w0     + w_family[k] ) 

Convert:

text alpha0_k beta0_k 

using the configured prior strength.

---

## Posterior Construction

Using:

text I_k C_k 

compute:

text alpha' beta' 

and posterior mean:

text m_hat 

---

## Target Logit

Compute:

text z_target = logit(m_hat) 

Desired adjustment:

text delta_star = z_target - w0 

---

## Learning Rate

Use evidence-scaled learning rate:

text eta = lambda * I / (I + s_eta) 

---

## Weight Update

Compute:

text delta_w = eta * (delta_star - w) 

Apply:

- clipping
- ridge decay

as described in the CTR framework.

---

## Family Centering

After all bucket updates within a family:

text mean(w_family) = 0 

must be enforced.

This keeps:

text w0 

interpretable as the global baseline.

---

## Global Baseline Update

Update:

text w0 

using global impressions and clicks.

Apply:

- Bayesian update
- confidence interval guard
- smoothing

as described in the CTR framework.

---

## Output

Write updated:

text w0  w_ad  w_dom  w_ctx 

to:

- Redis
- PostgreSQL

These weights become the priors used by the Flink CTR update process during the next update cycle.
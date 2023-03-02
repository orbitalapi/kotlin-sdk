

## Developing

## Releasing
Releases must be signed with a GPG key.  This is stored within the secrets in Github actions.

To generate:

```bash
gpg --gen-key
gpg --export-secret-keys --armor THE_KEY_ID

```

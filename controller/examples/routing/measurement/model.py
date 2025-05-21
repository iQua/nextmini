import torch
import torch.nn as nn
import pytorch_lightning as pl


class TransformerEncoder(nn.Module):
    def __init__(self, input_dim, num_heads, hidden_dim, num_layers):
        super(TransformerEncoder, self).__init__()

        self.hidden_dim = hidden_dim
        self.num_layers = num_layers

        self.normalization = nn.LayerNorm(input_dim)
        
        self.embedding = nn.Linear(input_dim, hidden_dim)
        self.normalization = nn.LayerNorm(input_dim)
        self.positional_encoding = PositionalEncoding(hidden_dim)

        encoder_layer = nn.TransformerEncoderLayer(hidden_dim, num_heads)
        self.transformer_encoder = nn.TransformerEncoder(encoder_layer, num_layers)

        self.readout = nn.Linear(hidden_dim, hidden_dim)
        self.readout2 = nn.Linear(hidden_dim, hidden_dim)

    def forward(self, x):
        x = self.normalization(x)
        x = self.embedding(x)
        x = self.positional_encoding(x)

        x = x.permute(1, 0, 2)  # [seq_len, batch_size, hidden_dim]
        x = self.transformer_encoder(x)
        x = x.permute(1, 0, 2)  # [batch_size, seq_len, hidden_dim]
        
        # seq_len = x.size(1)
        # x = x.reshape(-1, self.hidden_dim)  # [batch_size * seq_len, hidden_dim]
        # xx = x.T                            # [hidden_dim, batch_size * seq_len]
        # x = self.readout(x)
        xx = x.clone()
        x = self.readout(x)
        xx = self.readout2(xx).transpose(1,2)
        outer_product = torch.matmul(x, xx)  # [batch_size * seq_len, seq_len]
        # outer_product = outer_product.view(-1, seq_len, seq_len, self.hidden_dim)  # [batch_size, seq_len, seq_len, hidden_dim]

        # out = self.readout(outer_product).squeeze(-1)  # [batch_size, seq_len, seq_len]
        # out = torch.softmax(outer_product.view(outer_product.shape[0], outer_product.shape[1]*outer_product.shape[2]), dim=1)
        out = outer_product.reshape(outer_product.shape[0], outer_product.shape[1]*outer_product.shape[2])
        return out


class PositionalEncoding(nn.Module):
    def __init__(self, hidden_dim, max_length=1000):
        super(PositionalEncoding, self).__init__()

        position = torch.arange(0, max_length).unsqueeze(1)
        div_term = torch.exp(
            torch.arange(0, hidden_dim, 2) * (-torch.log(torch.tensor(10000.0)) / hidden_dim)
        )

        positional_encoding = torch.zeros(max_length, hidden_dim)
        positional_encoding[:, 0::2] = torch.sin(position * div_term)
        positional_encoding[:, 1::2] = torch.cos(position * div_term[:hidden_dim // 2])

        self.register_buffer('positional_encoding', positional_encoding)

    def forward(self, x):
        x = x + self.positional_encoding[:x.size(1), :]
        return x

class Model(pl.LightningModule):
    def __init__(self, model):
        super().__init__()
        self.model = model
         
    def forward(self, x):
        x = self.model(x)
        return x
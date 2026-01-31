import torch
from torch_geometric.data import Data
from torch_geometric.nn import GCNConv
import torch.nn.functional as F

def load_feature_vectors(file_path):
    vectors = {}
    with open(file_path, 'r') as file:
        lines = file.readlines()
        for line in lines:
            parts = line.split()
            if len(parts) > 1:
                vector_name = parts[0]
                vector_values = [float(x) for x in parts[1:]]
                vectors[vector_name] = vector_values
    return vectors

feature_file_path = 'features_mat.txt'
feature_vectors = load_feature_vectors(feature_file_path)

edge_list = []
y = []
with open("TrainingData.txt", "r") as f:
    next(f)
    for line in f:
        source, target, interaction = line.strip().split()
        edge_list.append((source, target))
        y.append(int(interaction))

nodes = set(feature_vectors.keys())
node_index = {node: i for i, node in enumerate(nodes)}

edge_index = torch.tensor([[node_index[src], node_index[tgt]] for src, tgt in edge_list], dtype=torch.long).t().contiguous()

num_features = len(next(iter(feature_vectors.values())))
x = torch.stack([
    torch.tensor(feature_vectors[node], dtype=torch.float32) if node in feature_vectors else torch.zeros(num_features)
    for node in nodes
])

class GCN(torch.nn.Module):
    def __init__(self, input_dim, hidden_dim):
        super(GCN, self).__init__()
        self.conv1 = GCNConv(input_dim, hidden_dim)
        self.conv2 = GCNConv(hidden_dim, hidden_dim)
        self.conv3 = GCNConv(hidden_dim, hidden_dim)

    def forward(self, x, edge_index):
        x = self.conv1(x, edge_index)
        x = F.relu(x)
        x = self.conv2(x, edge_index)
        x = F.relu(x)
        x = self.conv3(x, edge_index)
        return x

data = Data(x=x, edge_index=edge_index)
y = torch.tensor(y, dtype=torch.float32)
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model = GCN(input_dim=data.num_node_features, hidden_dim=32).to(device)
data = data.to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.05, weight_decay=5e-4)

def train():
    model.train()
    optimizer.zero_grad()

    node_embeddings = model(data.x, data.edge_index)
    edge_preds = torch.sigmoid((node_embeddings[edge_index[0]] * node_embeddings[edge_index[1]]).sum(dim=1))

    loss = F.mse_loss(edge_preds, y.to(device))
    loss.backward()
    optimizer.step()
    return loss.item(), edge_preds

def compute_accuracy(preds, labels, threshold=0.5):
    preds_class = (preds >= threshold).float()
    correct = (preds_class == labels).sum().item()
    return correct / len(labels)

best_accuracy = 0.0
best_weights_path = 'best_model_weights.pth'

for epoch in range(2000):
    loss, edge_preds = train()
    accuracy = compute_accuracy(edge_preds, y.to(device))

    # Save weights if accuracy improves
    if accuracy > best_accuracy:
        best_accuracy = accuracy
        torch.save(model.state_dict(), best_weights_path)

    if epoch % 100 == 0:
        print(f'Epoch {epoch}, Loss: {loss:.4f}, Accuracy: {accuracy:.4f}, Best Accuracy: {best_accuracy:.4f}')

# Load best weights for testing
model.load_state_dict(torch.load(best_weights_path))
model.eval()

with torch.no_grad():
    node_embeddings = model(data.x, data.edge_index)
    edge_preds = torch.sigmoid((node_embeddings[edge_index[0]] * node_embeddings[edge_index[1]]).sum(dim=1))
    test_accuracy = compute_accuracy(edge_preds, y.to(device))
    print(f"Test Accuracy: {test_accuracy:.4f}")

test_edge_list = []
test_y = []
with open("test_data.txt", "r") as f:
    next(f)
    for line in f:
        source, target, interaction = line.strip().split()
        test_edge_list.append((source, target))
        test_y.append(int(interaction))


test_edge_index = torch.tensor([[node_index[src], node_index[tgt]] for src, tgt in test_edge_list], dtype=torch.long).t().contiguous()


test_y = torch.tensor(test_y, dtype=torch.float32)


def test():
    # Load best weights for testing
    model.load_state_dict(torch.load(best_weights_path, weights_only=True))
    model.eval()
    with torch.no_grad():

        node_embeddings = model(data.x, data.edge_index)

        test_edge_preds = torch.sigmoid((node_embeddings[test_edge_index[0]] * node_embeddings[test_edge_index[1]]).sum(dim=1))

        test_edge_preds = torch.where(test_edge_preds >= 0.5, torch.tensor(1.0, device=device), torch.tensor(0.0, device=device))
        print(test_edge_preds)

        accuracy = (test_edge_preds == test_y.to(device)).sum().item() / test_y.size(0)


        print(f"Test Accuracy: {accuracy:.4f}")

test()

